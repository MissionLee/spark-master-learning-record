/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy.k8s

import io.fabric8.kubernetes.api.model.{LocalObjectReference, LocalObjectReferenceBuilder, Pod}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.{JavaMainAppResource, MainAppResource}
import org.apache.spark.internal.config.ConfigEntry

private[spark] sealed trait KubernetesRoleSpecificConf

/*
 * Structure containing metadata for Kubernetes logic that builds a Spark driver.
 */
private[spark] case class KubernetesDriverSpecificConf(
    mainAppResource: Option[MainAppResource],
    mainClass: String,
    appName: String,
    appArgs: Seq[String]) extends KubernetesRoleSpecificConf

/*
 * Structure containing metadata for Kubernetes logic that builds a Spark executor.
 */
private[spark] case class KubernetesExecutorSpecificConf(
    executorId: String,
    driverPod: Pod)
  extends KubernetesRoleSpecificConf

/**
 * Structure containing metadata for Kubernetes logic to build Spark pods.
 */
private[spark] case class KubernetesConf[T <: KubernetesRoleSpecificConf](
    sparkConf: SparkConf,
    roleSpecificConf: T,
    appResourceNamePrefix: String,
    appId: String,
    roleLabels: Map[String, String],
    roleAnnotations: Map[String, String],
    roleSecretNamesToMountPaths: Map[String, String],
    roleEnvs: Map[String, String]) {

  def namespace(): String = sparkConf.get(KUBERNETES_NAMESPACE)

  def sparkJars(): Seq[String] = sparkConf
    .getOption("spark.jars")
    .map(str => str.split(",").toSeq)
    .getOrElse(Seq.empty[String])

  def sparkFiles(): Seq[String] = sparkConf
    .getOption("spark.files")
    .map(str => str.split(",").toSeq)
    .getOrElse(Seq.empty[String])

  def imagePullPolicy(): String = sparkConf.get(CONTAINER_IMAGE_PULL_POLICY)

  def imagePullSecrets(): Seq[LocalObjectReference] = {
    sparkConf
      .get(IMAGE_PULL_SECRETS)
      .map(_.split(","))
      .getOrElse(Array.empty[String])
      .map(_.trim)
      .map { secret =>
        new LocalObjectReferenceBuilder().withName(secret).build()
      }
  }

  def nodeSelector(): Map[String, String] =
    KubernetesUtils.parsePrefixedKeyValuePairs(sparkConf, KUBERNETES_NODE_SELECTOR_PREFIX)

  def get[T](config: ConfigEntry[T]): T = sparkConf.get(config)

  def get(conf: String): String = sparkConf.get(conf)

  def get(conf: String, defaultValue: String): String = sparkConf.get(conf, defaultValue)

  def getOption(key: String): Option[String] = sparkConf.getOption(key)
}

private[spark] object KubernetesConf {
  def createDriverConf(
      sparkConf: SparkConf,
      appName: String,
      appResourceNamePrefix: String,
      appId: String,
      mainAppResource: Option[MainAppResource],
      mainClass: String,
      appArgs: Array[String]): KubernetesConf[KubernetesDriverSpecificConf] = {
    val sparkConfWithMainAppJar = sparkConf.clone()
    mainAppResource.foreach {
      case JavaMainAppResource(res) =>
        val previousJars = sparkConf
          .getOption("spark.jars")
          .map(_.split(","))
          .getOrElse(Array.empty)
        if (!previousJars.contains(res)) {
          sparkConfWithMainAppJar.setJars(previousJars ++ Seq(res))
        }
    }

    val driverCustomLabels = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf, KUBERNETES_DRIVER_LABEL_PREFIX)
    require(!driverCustomLabels.contains(SPARK_APP_ID_LABEL), "Label with key " +
      s"$SPARK_APP_ID_LABEL is not allowed as it is reserved for Spark bookkeeping " +
      "operations.")
    require(!driverCustomLabels.contains(SPARK_ROLE_LABEL), "Label with key " +
      s"$SPARK_ROLE_LABEL is not allowed as it is reserved for Spark bookkeeping " +
      "operations.")
    val driverLabels = driverCustomLabels ++ Map(
      SPARK_APP_ID_LABEL -> appId,
      SPARK_ROLE_LABEL -> SPARK_POD_DRIVER_ROLE)
    val driverAnnotations = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf, KUBERNETES_DRIVER_ANNOTATION_PREFIX)
    val driverSecretNamesToMountPaths = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf, KUBERNETES_DRIVER_SECRETS_PREFIX)
    val driverEnvs = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf, KUBERNETES_DRIVER_ENV_PREFIX)

    KubernetesConf(
      sparkConfWithMainAppJar,
      KubernetesDriverSpecificConf(mainAppResource, mainClass, appName, appArgs),
      appResourceNamePrefix,
      appId,
      driverLabels,
      driverAnnotations,
      driverSecretNamesToMountPaths,
      driverEnvs)
  }

  def createExecutorConf(
      sparkConf: SparkConf,
      executorId: String,
      appId: String,
      driverPod: Pod): KubernetesConf[KubernetesExecutorSpecificConf] = {
    val executorCustomLabels = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf, KUBERNETES_EXECUTOR_LABEL_PREFIX)
    require(
      !executorCustomLabels.contains(SPARK_APP_ID_LABEL),
      s"Custom executor labels cannot contain $SPARK_APP_ID_LABEL as it is reserved for Spark.")
    require(
      !executorCustomLabels.contains(SPARK_EXECUTOR_ID_LABEL),
      s"Custom executor labels cannot contain $SPARK_EXECUTOR_ID_LABEL as it is reserved for" +
        " Spark.")
    require(
      !executorCustomLabels.contains(SPARK_ROLE_LABEL),
      s"Custom executor labels cannot contain $SPARK_ROLE_LABEL as it is reserved for Spark.")
    val executorLabels = Map(
      SPARK_EXECUTOR_ID_LABEL -> executorId,
      SPARK_APP_ID_LABEL -> appId,
      SPARK_ROLE_LABEL -> SPARK_POD_EXECUTOR_ROLE) ++
      executorCustomLabels
    val executorAnnotations = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf, KUBERNETES_EXECUTOR_ANNOTATION_PREFIX)
    val executorSecrets = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf, KUBERNETES_EXECUTOR_SECRETS_PREFIX)
    val executorEnv = sparkConf.getExecutorEnv.toMap

    KubernetesConf(
      sparkConf.clone(),
      KubernetesExecutorSpecificConf(executorId, driverPod),
      sparkConf.get(KUBERNETES_EXECUTOR_POD_NAME_PREFIX),
      appId,
      executorLabels,
      executorAnnotations,
      executorSecrets,
      executorEnv)
  }
}
