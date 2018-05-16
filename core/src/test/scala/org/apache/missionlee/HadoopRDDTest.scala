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
package org.apache.missionlee

import org.apache.spark.{SparkConf, SparkContext}


object HadoopRDDTest {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("hadoop rdd test"))
    val rdd = sc.textFile("hdfs://localhost:9000/lms/spark/20180302_sparkJsonTest")
    rdd
      .flatMap(_.split(" "))
      .filter(_.length > 2)
      .map(_ => (_, 1))

      .foreach(
        // scalastyle:off
        Console.println
        // scalastyle:on
      )
    // .reduceByKey(_ + _)
  }
}
