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
package org.apache.spark.scheduler.cluster.k8s

import java.io.File

import io.fabric8.kubernetes.client.Config

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.deploy.k8s.{KubernetesUtils, SparkKubernetesClientFactory}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{ExternalClusterManager, SchedulerBackend, TaskScheduler, TaskSchedulerImpl}
import org.apache.spark.util.ThreadUtils

private[spark] class KubernetesClusterManager extends ExternalClusterManager with Logging {

  override def canCreate(masterURL: String): Boolean = masterURL.startsWith("k8s")

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    if (masterURL.startsWith("k8s") &&
      sc.deployMode == "client" &&
      !sc.conf.get(KUBERNETES_DRIVER_SUBMIT_CHECK).getOrElse(false)) {
      throw new SparkException("Client mode is currently not supported for Kubernetes.")
    }

    new TaskSchedulerImpl(sc)
  }

  override def createSchedulerBackend(
      sc: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler): SchedulerBackend = {
    val executorSecretNamesToMountPaths = KubernetesUtils.parsePrefixedKeyValuePairs(
      sc.conf, KUBERNETES_EXECUTOR_SECRETS_PREFIX)
    val kubernetesClient = SparkKubernetesClientFactory.createKubernetesClient(
      KUBERNETES_MASTER_INTERNAL_URL,
      Some(sc.conf.get(KUBERNETES_NAMESPACE)),
      KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX,
      sc.conf,
      Some(new File(Config.KUBERNETES_SERVICE_ACCOUNT_TOKEN_PATH)),
      Some(new File(Config.KUBERNETES_SERVICE_ACCOUNT_CA_CRT_PATH)))

    val allocatorExecutor = ThreadUtils
      .newDaemonSingleThreadScheduledExecutor("kubernetes-pod-allocator")
    val requestExecutorsService = ThreadUtils.newDaemonCachedThreadPool(
      "kubernetes-executor-requests")
    new KubernetesClusterSchedulerBackend(
      scheduler.asInstanceOf[TaskSchedulerImpl],
      sc.env.rpcEnv,
      new KubernetesExecutorBuilder,
      kubernetesClient,
      allocatorExecutor,
      requestExecutorsService)
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
  }
}
