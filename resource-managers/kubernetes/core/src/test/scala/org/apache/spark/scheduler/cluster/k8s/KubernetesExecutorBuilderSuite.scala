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

import io.fabric8.kubernetes.api.model.PodBuilder

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesExecutorSpecificConf, SparkPod}
import org.apache.spark.deploy.k8s.features.{BasicExecutorFeatureStep, KubernetesFeaturesTestUtils, MountSecretsFeatureStep}

class KubernetesExecutorBuilderSuite extends SparkFunSuite {
  private val BASIC_STEP_TYPE = "basic"
  private val SECRETS_STEP_TYPE = "mount-secrets"

  private val basicFeatureStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    BASIC_STEP_TYPE, classOf[BasicExecutorFeatureStep])
  private val mountSecretsStep = KubernetesFeaturesTestUtils.getMockConfigStepForStepType(
    SECRETS_STEP_TYPE, classOf[MountSecretsFeatureStep])

  private val builderUnderTest = new KubernetesExecutorBuilder(
    _ => basicFeatureStep,
    _ => mountSecretsStep)

  test("Basic steps are consistently applied.") {
    val conf = KubernetesConf(
      new SparkConf(false),
      KubernetesExecutorSpecificConf(
        "executor-id", new PodBuilder().build()),
      "prefix",
      "appId",
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty)
    validateStepTypesApplied(builderUnderTest.buildFromFeatures(conf), BASIC_STEP_TYPE)
  }

  test("Apply secrets step if secrets are present.") {
    val conf = KubernetesConf(
      new SparkConf(false),
      KubernetesExecutorSpecificConf(
        "executor-id", new PodBuilder().build()),
      "prefix",
      "appId",
      Map.empty,
      Map.empty,
      Map("secret" -> "secretMountPath"),
      Map.empty)
    validateStepTypesApplied(
      builderUnderTest.buildFromFeatures(conf),
      BASIC_STEP_TYPE,
      SECRETS_STEP_TYPE)
  }

  private def validateStepTypesApplied(resolvedPod: SparkPod, stepTypes: String*): Unit = {
    assert(resolvedPod.pod.getMetadata.getLabels.size === stepTypes.size)
    stepTypes.foreach { stepType =>
      assert(resolvedPod.pod.getMetadata.getLabels.get(stepType) === stepType)
    }
  }
}
