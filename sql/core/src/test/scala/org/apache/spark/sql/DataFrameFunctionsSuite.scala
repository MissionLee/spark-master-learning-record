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

package org.apache.spark.sql

import java.nio.charset.StandardCharsets

import scala.util.Random

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

/**
 * Test suite for functions in [[org.apache.spark.sql.functions]].
 */
class DataFrameFunctionsSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("array with column name") {
    val df = Seq((0, 1)).toDF("a", "b")
    val row = df.select(array("a", "b")).first()

    val expectedType = ArrayType(IntegerType, containsNull = false)
    assert(row.schema(0).dataType === expectedType)
    assert(row.getAs[Seq[Int]](0) === Seq(0, 1))
  }

  test("array with column expression") {
    val df = Seq((0, 1)).toDF("a", "b")
    val row = df.select(array(col("a"), col("b") + col("b"))).first()

    val expectedType = ArrayType(IntegerType, containsNull = false)
    assert(row.schema(0).dataType === expectedType)
    assert(row.getSeq[Int](0) === Seq(0, 2))
  }

  test("map with column expressions") {
    val df = Seq(1 -> "a").toDF("a", "b")
    val row = df.select(map($"a" + 1, $"b")).first()

    val expectedType = MapType(IntegerType, StringType, valueContainsNull = true)
    assert(row.schema(0).dataType === expectedType)
    assert(row.getMap[Int, String](0) === Map(2 -> "a"))
  }

  test("struct with column name") {
    val df = Seq((1, "str")).toDF("a", "b")
    val row = df.select(struct("a", "b")).first()

    val expectedType = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("b", StringType)
    ))
    assert(row.schema(0).dataType === expectedType)
    assert(row.getAs[Row](0) === Row(1, "str"))
  }

  test("struct with column expression") {
    val df = Seq((1, "str")).toDF("a", "b")
    val row = df.select(struct((col("a") * 2).as("c"), col("b"))).first()

    val expectedType = StructType(Seq(
      StructField("c", IntegerType, nullable = false),
      StructField("b", StringType)
    ))
    assert(row.schema(0).dataType === expectedType)
    assert(row.getAs[Row](0) === Row(2, "str"))
  }

  test("struct with column expression to be automatically named") {
    val df = Seq((1, "str")).toDF("a", "b")
    val result = df.select(struct((col("a") * 2), col("b")))

    val expectedType = StructType(Seq(
      StructField("col1", IntegerType, nullable = false),
      StructField("b", StringType)
    ))
    assert(result.first.schema(0).dataType === expectedType)
    checkAnswer(result, Row(Row(2, "str")))
  }

  test("struct with literal columns") {
    val df = Seq((1, "str1"), (2, "str2")).toDF("a", "b")
    val result = df.select(struct((col("a") * 2), lit(5.0)))

    val expectedType = StructType(Seq(
      StructField("col1", IntegerType, nullable = false),
      StructField("col2", DoubleType, nullable = false)
    ))

    assert(result.first.schema(0).dataType === expectedType)
    checkAnswer(result, Seq(Row(Row(2, 5.0)), Row(Row(4, 5.0))))
  }

  test("struct with all literal columns") {
    val df = Seq((1, "str1"), (2, "str2")).toDF("a", "b")
    val result = df.select(struct(lit("v"), lit(5.0)))

    val expectedType = StructType(Seq(
      StructField("col1", StringType, nullable = false),
      StructField("col2", DoubleType, nullable = false)
    ))

    assert(result.first.schema(0).dataType === expectedType)
    checkAnswer(result, Seq(Row(Row("v", 5.0)), Row(Row("v", 5.0))))
  }

  test("constant functions") {
    checkAnswer(
      sql("SELECT E()"),
      Row(scala.math.E)
    )
    checkAnswer(
      sql("SELECT PI()"),
      Row(scala.math.Pi)
    )
  }

  test("bitwiseNOT") {
    checkAnswer(
      testData2.select(bitwiseNOT($"a")),
      testData2.collect().toSeq.map(r => Row(~r.getInt(0))))
  }

  test("bin") {
    val df = Seq[(Integer, Integer)]((12, null)).toDF("a", "b")
    checkAnswer(
      df.select(bin("a"), bin("b")),
      Row("1100", null))
    checkAnswer(
      df.selectExpr("bin(a)", "bin(b)"),
      Row("1100", null))
  }

  test("if function") {
    val df = Seq((1, 2)).toDF("a", "b")
    checkAnswer(
      df.selectExpr("if(a = 1, 'one', 'not_one')", "if(b = 1, 'one', 'not_one')"),
      Row("one", "not_one"))
  }

  test("misc md5 function") {
    val df = Seq(("ABC", Array[Byte](1, 2, 3, 4, 5, 6))).toDF("a", "b")
    checkAnswer(
      df.select(md5($"a"), md5($"b")),
      Row("902fbdd2b1df0c4f70b4a5d23525e932", "6ac1e56bc78f031059be7be854522c4c"))

    checkAnswer(
      df.selectExpr("md5(a)", "md5(b)"),
      Row("902fbdd2b1df0c4f70b4a5d23525e932", "6ac1e56bc78f031059be7be854522c4c"))
  }

  test("misc sha1 function") {
    val df = Seq(("ABC", "ABC".getBytes(StandardCharsets.UTF_8))).toDF("a", "b")
    checkAnswer(
      df.select(sha1($"a"), sha1($"b")),
      Row("3c01bdbb26f358bab27f267924aa2c9a03fcfdb8", "3c01bdbb26f358bab27f267924aa2c9a03fcfdb8"))

    val dfEmpty = Seq(("", "".getBytes(StandardCharsets.UTF_8))).toDF("a", "b")
    checkAnswer(
      dfEmpty.selectExpr("sha1(a)", "sha1(b)"),
      Row("da39a3ee5e6b4b0d3255bfef95601890afd80709", "da39a3ee5e6b4b0d3255bfef95601890afd80709"))
  }

  test("misc sha2 function") {
    val df = Seq(("ABC", Array[Byte](1, 2, 3, 4, 5, 6))).toDF("a", "b")
    checkAnswer(
      df.select(sha2($"a", 256), sha2($"b", 256)),
      Row("b5d4045c3f466fa91fe2cc6abe79232a1a57cdf104f7a26e716e0a1e2789df78",
        "7192385c3c0605de55bb9476ce1d90748190ecb32a8eed7f5207b30cf6a1fe89"))

    checkAnswer(
      df.selectExpr("sha2(a, 256)", "sha2(b, 256)"),
      Row("b5d4045c3f466fa91fe2cc6abe79232a1a57cdf104f7a26e716e0a1e2789df78",
        "7192385c3c0605de55bb9476ce1d90748190ecb32a8eed7f5207b30cf6a1fe89"))

    intercept[IllegalArgumentException] {
      df.select(sha2($"a", 1024))
    }
  }

  test("misc crc32 function") {
    val df = Seq(("ABC", Array[Byte](1, 2, 3, 4, 5, 6))).toDF("a", "b")
    checkAnswer(
      df.select(crc32($"a"), crc32($"b")),
      Row(2743272264L, 2180413220L))

    checkAnswer(
      df.selectExpr("crc32(a)", "crc32(b)"),
      Row(2743272264L, 2180413220L))
  }

  test("string function find_in_set") {
    val df = Seq(("abc,b,ab,c,def", "abc,b,ab,c,def")).toDF("a", "b")

    checkAnswer(
      df.selectExpr("find_in_set('ab', a)", "find_in_set('x', b)"),
      Row(3, 0))
  }

  test("conditional function: least") {
    checkAnswer(
      testData2.select(least(lit(-1), lit(0), col("a"), col("b"))).limit(1),
      Row(-1)
    )
    checkAnswer(
      sql("SELECT least(a, 2) as l from testData2 order by l"),
      Seq(Row(1), Row(1), Row(2), Row(2), Row(2), Row(2))
    )
  }

  test("conditional function: greatest") {
    checkAnswer(
      testData2.select(greatest(lit(2), lit(3), col("a"), col("b"))).limit(1),
      Row(3)
    )
    checkAnswer(
      sql("SELECT greatest(a, 2) as g from testData2 order by g"),
      Seq(Row(2), Row(2), Row(2), Row(2), Row(3), Row(3))
    )
  }

  test("pmod") {
    val intData = Seq((7, 3), (-7, 3)).toDF("a", "b")
    checkAnswer(
      intData.select(pmod('a, 'b)),
      Seq(Row(1), Row(2))
    )
    checkAnswer(
      intData.select(pmod('a, lit(3))),
      Seq(Row(1), Row(2))
    )
    checkAnswer(
      intData.select(pmod(lit(-7), 'b)),
      Seq(Row(2), Row(2))
    )
    checkAnswer(
      intData.selectExpr("pmod(a, b)"),
      Seq(Row(1), Row(2))
    )
    checkAnswer(
      intData.selectExpr("pmod(a, 3)"),
      Seq(Row(1), Row(2))
    )
    checkAnswer(
      intData.selectExpr("pmod(-7, b)"),
      Seq(Row(2), Row(2))
    )
    val doubleData = Seq((7.2, 4.1)).toDF("a", "b")
    checkAnswer(
      doubleData.select(pmod('a, 'b)),
      Seq(Row(3.1000000000000005)) // same as hive
    )
    checkAnswer(
      doubleData.select(pmod(lit(2), lit(Int.MaxValue))),
      Seq(Row(2))
    )
  }

  test("sort_array/array_sort functions") {
    val df = Seq(
      (Array[Int](2, 1, 3), Array("b", "c", "a")),
      (Array.empty[Int], Array.empty[String]),
      (null, null)
    ).toDF("a", "b")
    checkAnswer(
      df.select(sort_array($"a"), sort_array($"b")),
      Seq(
        Row(Seq(1, 2, 3), Seq("a", "b", "c")),
        Row(Seq.empty[Int], Seq.empty[String]),
        Row(null, null))
    )
    checkAnswer(
      df.select(sort_array($"a", false), sort_array($"b", false)),
      Seq(
        Row(Seq(3, 2, 1), Seq("c", "b", "a")),
        Row(Seq.empty[Int], Seq.empty[String]),
        Row(null, null))
    )
    checkAnswer(
      df.selectExpr("sort_array(a)", "sort_array(b)"),
      Seq(
        Row(Seq(1, 2, 3), Seq("a", "b", "c")),
        Row(Seq.empty[Int], Seq.empty[String]),
        Row(null, null))
    )
    checkAnswer(
      df.selectExpr("sort_array(a, true)", "sort_array(b, false)"),
      Seq(
        Row(Seq(1, 2, 3), Seq("c", "b", "a")),
        Row(Seq.empty[Int], Seq.empty[String]),
        Row(null, null))
    )

    val df2 = Seq((Array[Array[Int]](Array(2), Array(1), Array(2, 4), null), "x")).toDF("a", "b")
    checkAnswer(
      df2.selectExpr("sort_array(a, true)", "sort_array(a, false)"),
      Seq(
        Row(
          Seq[Seq[Int]](null, Seq(1), Seq(2), Seq(2, 4)),
          Seq[Seq[Int]](Seq(2, 4), Seq(2), Seq(1), null)))
    )

    val df3 = Seq(("xxx", "x")).toDF("a", "b")
    assert(intercept[AnalysisException] {
      df3.selectExpr("sort_array(a)").collect()
    }.getMessage().contains("only supports array input"))

    checkAnswer(
      df.select(array_sort($"a"), array_sort($"b")),
      Seq(
        Row(Seq(1, 2, 3), Seq("a", "b", "c")),
        Row(Seq.empty[Int], Seq.empty[String]),
        Row(null, null))
    )
    checkAnswer(
      df.selectExpr("array_sort(a)", "array_sort(b)"),
      Seq(
        Row(Seq(1, 2, 3), Seq("a", "b", "c")),
        Row(Seq.empty[Int], Seq.empty[String]),
        Row(null, null))
    )

    checkAnswer(
      df2.selectExpr("array_sort(a)"),
      Seq(Row(Seq[Seq[Int]](Seq(1), Seq(2), Seq(2, 4), null)))
    )

    assert(intercept[AnalysisException] {
      df3.selectExpr("array_sort(a)").collect()
    }.getMessage().contains("only supports array input"))
  }

  test("array size function") {
    val df = Seq(
      (Seq[Int](1, 2), "x"),
      (Seq[Int](), "y"),
      (Seq[Int](1, 2, 3), "z"),
      (null, "empty")
    ).toDF("a", "b")
    checkAnswer(
      df.select(size($"a")),
      Seq(Row(2), Row(0), Row(3), Row(-1))
    )
    checkAnswer(
      df.selectExpr("size(a)"),
      Seq(Row(2), Row(0), Row(3), Row(-1))
    )

    checkAnswer(
      df.selectExpr("cardinality(a)"),
      Seq(Row(2L), Row(0L), Row(3L), Row(-1L))
    )
  }

  test("map size function") {
    val df = Seq(
      (Map[Int, Int](1 -> 1, 2 -> 2), "x"),
      (Map[Int, Int](), "y"),
      (Map[Int, Int](1 -> 1, 2 -> 2, 3 -> 3), "z"),
      (null, "empty")
    ).toDF("a", "b")
    checkAnswer(
      df.select(size($"a")),
      Seq(Row(2), Row(0), Row(3), Row(-1))
    )
    checkAnswer(
      df.selectExpr("size(a)"),
      Seq(Row(2), Row(0), Row(3), Row(-1))
    )
  }

  test("map_keys/map_values function") {
    val df = Seq(
      (Map[Int, Int](1 -> 100, 2 -> 200), "x"),
      (Map[Int, Int](), "y"),
      (Map[Int, Int](1 -> 100, 2 -> 200, 3 -> 300), "z")
    ).toDF("a", "b")
    checkAnswer(
      df.selectExpr("map_keys(a)"),
      Seq(Row(Seq(1, 2)), Row(Seq.empty), Row(Seq(1, 2, 3)))
    )
    checkAnswer(
      df.selectExpr("map_values(a)"),
      Seq(Row(Seq(100, 200)), Row(Seq.empty), Row(Seq(100, 200, 300)))
    )
  }

  test("array contains function") {
    val df = Seq(
      (Seq[Int](1, 2), "x"),
      (Seq[Int](), "x")
    ).toDF("a", "b")

    // Simple test cases
    checkAnswer(
      df.select(array_contains(df("a"), 1)),
      Seq(Row(true), Row(false))
    )
    checkAnswer(
      df.selectExpr("array_contains(a, 1)"),
      Seq(Row(true), Row(false))
    )

    // In hive, this errors because null has no type information
    intercept[AnalysisException] {
      df.select(array_contains(df("a"), null))
    }
    intercept[AnalysisException] {
      df.selectExpr("array_contains(a, null)")
    }
    intercept[AnalysisException] {
      df.selectExpr("array_contains(null, 1)")
    }

    checkAnswer(
      df.selectExpr("array_contains(array(array(1), null)[0], 1)"),
      Seq(Row(true), Row(true))
    )
    checkAnswer(
      df.selectExpr("array_contains(array(1, null), array(1, null)[0])"),
      Seq(Row(true), Row(true))
    )
  }

  test("slice function") {
    val df = Seq(
      Seq(1, 2, 3),
      Seq(4, 5)
    ).toDF("x")

    val answer = Seq(Row(Seq(2, 3)), Row(Seq(5)))

    checkAnswer(df.select(slice(df("x"), 2, 2)), answer)
    checkAnswer(df.selectExpr("slice(x, 2, 2)"), answer)

    val answerNegative = Seq(Row(Seq(3)), Row(Seq(5)))
    checkAnswer(df.select(slice(df("x"), -1, 1)), answerNegative)
    checkAnswer(df.selectExpr("slice(x, -1, 1)"), answerNegative)
  }

  test("array_join function") {
    val df = Seq(
      (Seq[String]("a", "b"), ","),
      (Seq[String]("a", null, "b"), ","),
      (Seq.empty[String], ",")
    ).toDF("x", "delimiter")

    checkAnswer(
      df.select(array_join(df("x"), ";")),
      Seq(Row("a;b"), Row("a;b"), Row(""))
    )
    checkAnswer(
      df.select(array_join(df("x"), ";", "NULL")),
      Seq(Row("a;b"), Row("a;NULL;b"), Row(""))
    )
    checkAnswer(
      df.selectExpr("array_join(x, delimiter)"),
      Seq(Row("a,b"), Row("a,b"), Row("")))
    checkAnswer(
      df.selectExpr("array_join(x, delimiter, 'NULL')"),
      Seq(Row("a,b"), Row("a,NULL,b"), Row("")))
  }

  test("array_min function") {
    val df = Seq(
      Seq[Option[Int]](Some(1), Some(3), Some(2)),
      Seq.empty[Option[Int]],
      Seq[Option[Int]](None),
      Seq[Option[Int]](None, Some(1), Some(-100))
    ).toDF("a")

    val answer = Seq(Row(1), Row(null), Row(null), Row(-100))

    checkAnswer(df.select(array_min(df("a"))), answer)
    checkAnswer(df.selectExpr("array_min(a)"), answer)
  }

  test("array_max function") {
    val df = Seq(
      Seq[Option[Int]](Some(1), Some(3), Some(2)),
      Seq.empty[Option[Int]],
      Seq[Option[Int]](None),
      Seq[Option[Int]](None, Some(1), Some(-100))
    ).toDF("a")

    val answer = Seq(Row(3), Row(null), Row(null), Row(1))

    checkAnswer(df.select(array_max(df("a"))), answer)
    checkAnswer(df.selectExpr("array_max(a)"), answer)
  }

  test("reverse function") {
    val dummyFilter = (c: Column) => c.isNull || c.isNotNull // switch codegen on

    // String test cases
    val oneRowDF = Seq(("Spark", 3215)).toDF("s", "i")

    checkAnswer(
      oneRowDF.select(reverse('s)),
      Seq(Row("krapS"))
    )
    checkAnswer(
      oneRowDF.selectExpr("reverse(s)"),
      Seq(Row("krapS"))
    )
    checkAnswer(
      oneRowDF.select(reverse('i)),
      Seq(Row("5123"))
    )
    checkAnswer(
      oneRowDF.selectExpr("reverse(i)"),
      Seq(Row("5123"))
    )
    checkAnswer(
      oneRowDF.selectExpr("reverse(null)"),
      Seq(Row(null))
    )

    // Array test cases (primitive-type elements)
    val idf = Seq(
      Seq(1, 9, 8, 7),
      Seq(5, 8, 9, 7, 2),
      Seq.empty,
      null
    ).toDF("i")

    checkAnswer(
      idf.select(reverse('i)),
      Seq(Row(Seq(7, 8, 9, 1)), Row(Seq(2, 7, 9, 8, 5)), Row(Seq.empty), Row(null))
    )
    checkAnswer(
      idf.filter(dummyFilter('i)).select(reverse('i)),
      Seq(Row(Seq(7, 8, 9, 1)), Row(Seq(2, 7, 9, 8, 5)), Row(Seq.empty), Row(null))
    )
    checkAnswer(
      idf.selectExpr("reverse(i)"),
      Seq(Row(Seq(7, 8, 9, 1)), Row(Seq(2, 7, 9, 8, 5)), Row(Seq.empty), Row(null))
    )
    checkAnswer(
      oneRowDF.selectExpr("reverse(array(1, null, 2, null))"),
      Seq(Row(Seq(null, 2, null, 1)))
    )
    checkAnswer(
      oneRowDF.filter(dummyFilter('i)).selectExpr("reverse(array(1, null, 2, null))"),
      Seq(Row(Seq(null, 2, null, 1)))
    )

    // Array test cases (non-primitive-type elements)
    val sdf = Seq(
      Seq("c", "a", "b"),
      Seq("b", null, "c", null),
      Seq.empty,
      null
    ).toDF("s")

    checkAnswer(
      sdf.select(reverse('s)),
      Seq(Row(Seq("b", "a", "c")), Row(Seq(null, "c", null, "b")), Row(Seq.empty), Row(null))
    )
    checkAnswer(
      sdf.filter(dummyFilter('s)).select(reverse('s)),
      Seq(Row(Seq("b", "a", "c")), Row(Seq(null, "c", null, "b")), Row(Seq.empty), Row(null))
    )
    checkAnswer(
      sdf.selectExpr("reverse(s)"),
      Seq(Row(Seq("b", "a", "c")), Row(Seq(null, "c", null, "b")), Row(Seq.empty), Row(null))
    )
    checkAnswer(
      oneRowDF.selectExpr("reverse(array(array(1, 2), array(3, 4)))"),
      Seq(Row(Seq(Seq(3, 4), Seq(1, 2))))
    )
    checkAnswer(
      oneRowDF.filter(dummyFilter('s)).selectExpr("reverse(array(array(1, 2), array(3, 4)))"),
      Seq(Row(Seq(Seq(3, 4), Seq(1, 2))))
    )

    // Error test cases
    intercept[AnalysisException] {
      oneRowDF.selectExpr("reverse(struct(1, 'a'))")
    }
    intercept[AnalysisException] {
      oneRowDF.selectExpr("reverse(map(1, 'a'))")
    }
  }

  test("array position function") {
    val df = Seq(
      (Seq[Int](1, 2), "x"),
      (Seq[Int](), "x")
    ).toDF("a", "b")

    checkAnswer(
      df.select(array_position(df("a"), 1)),
      Seq(Row(1L), Row(0L))
    )
    checkAnswer(
      df.selectExpr("array_position(a, 1)"),
      Seq(Row(1L), Row(0L))
    )

    checkAnswer(
      df.select(array_position(df("a"), null)),
      Seq(Row(null), Row(null))
    )
    checkAnswer(
      df.selectExpr("array_position(a, null)"),
      Seq(Row(null), Row(null))
    )

    checkAnswer(
      df.selectExpr("array_position(array(array(1), null)[0], 1)"),
      Seq(Row(1L), Row(1L))
    )
    checkAnswer(
      df.selectExpr("array_position(array(1, null), array(1, null)[0])"),
      Seq(Row(1L), Row(1L))
    )
  }

  test("element_at function") {
    val df = Seq(
      (Seq[String]("1", "2", "3")),
      (Seq[String](null, "")),
      (Seq[String]())
    ).toDF("a")

    intercept[Exception] {
      checkAnswer(
        df.select(element_at(df("a"), 0)),
        Seq(Row(null), Row(null), Row(null))
      )
    }.getMessage.contains("SQL array indices start at 1")
    intercept[Exception] {
      checkAnswer(
        df.select(element_at(df("a"), 1.1)),
        Seq(Row(null), Row(null), Row(null))
      )
    }
    checkAnswer(
      df.select(element_at(df("a"), 4)),
      Seq(Row(null), Row(null), Row(null))
    )

    checkAnswer(
      df.select(element_at(df("a"), 1)),
      Seq(Row("1"), Row(null), Row(null))
    )
    checkAnswer(
      df.select(element_at(df("a"), -1)),
      Seq(Row("3"), Row(""), Row(null))
    )

    checkAnswer(
      df.selectExpr("element_at(a, 4)"),
      Seq(Row(null), Row(null), Row(null))
    )

    checkAnswer(
      df.selectExpr("element_at(a, 1)"),
      Seq(Row("1"), Row(null), Row(null))
    )
    checkAnswer(
      df.selectExpr("element_at(a, -1)"),
      Seq(Row("3"), Row(""), Row(null))
    )
  }

  test("concat function - arrays") {
    val nseqi : Seq[Int] = null
    val nseqs : Seq[String] = null
    val df = Seq(

      (Seq(1), Seq(2, 3), Seq(5L, 6L), nseqi, Seq("a", "b", "c"), Seq("d", "e"), Seq("f"), nseqs),
      (Seq(1, 0), Seq.empty[Int], Seq(2L), nseqi, Seq("a"), Seq.empty[String], Seq(null), nseqs)
    ).toDF("i1", "i2", "i3", "in", "s1", "s2", "s3", "sn")

    val dummyFilter = (c: Column) => c.isNull || c.isNotNull // switch codeGen on

    // Simple test cases
    checkAnswer(
      df.selectExpr("array(1, 2, 3L)"),
      Seq(Row(Seq(1L, 2L, 3L)), Row(Seq(1L, 2L, 3L)))
    )

    checkAnswer (
      df.select(concat($"i1", $"s1")),
      Seq(Row(Seq("1", "a", "b", "c")), Row(Seq("1", "0", "a")))
    )
    checkAnswer(
      df.select(concat($"i1", $"i2", $"i3")),
      Seq(Row(Seq(1, 2, 3, 5, 6)), Row(Seq(1, 0, 2)))
    )
    checkAnswer(
      df.filter(dummyFilter($"i1")).select(concat($"i1", $"i2", $"i3")),
      Seq(Row(Seq(1, 2, 3, 5, 6)), Row(Seq(1, 0, 2)))
    )
    checkAnswer(
      df.selectExpr("concat(array(1, null), i2, i3)"),
      Seq(Row(Seq(1, null, 2, 3, 5, 6)), Row(Seq(1, null, 2)))
    )
    checkAnswer(
      df.select(concat($"s1", $"s2", $"s3")),
      Seq(Row(Seq("a", "b", "c", "d", "e", "f")), Row(Seq("a", null)))
    )
    checkAnswer(
      df.selectExpr("concat(s1, s2, s3)"),
      Seq(Row(Seq("a", "b", "c", "d", "e", "f")), Row(Seq("a", null)))
    )
    checkAnswer(
      df.filter(dummyFilter($"s1"))select(concat($"s1", $"s2", $"s3")),
      Seq(Row(Seq("a", "b", "c", "d", "e", "f")), Row(Seq("a", null)))
    )

    // Null test cases
    checkAnswer(
      df.select(concat($"i1", $"in")),
      Seq(Row(null), Row(null))
    )
    checkAnswer(
      df.select(concat($"in", $"i1")),
      Seq(Row(null), Row(null))
    )
    checkAnswer(
      df.select(concat($"s1", $"sn")),
      Seq(Row(null), Row(null))
    )
    checkAnswer(
      df.select(concat($"sn", $"s1")),
      Seq(Row(null), Row(null))
    )

    // Type error test cases
    intercept[AnalysisException] {
      df.selectExpr("concat(i1, i2, null)")
    }

    intercept[AnalysisException] {
      df.selectExpr("concat(i1, array(i1, i2))")
    }

    val e = intercept[AnalysisException] {
      df.selectExpr("concat(map(1, 2), map(3, 4))")
    }
    assert(e.getMessage.contains("string, binary or array"))
  }

  test("flatten function") {
    val dummyFilter = (c: Column) => c.isNull || c.isNotNull // to switch codeGen on
    val oneRowDF = Seq((1, "a", Seq(1, 2, 3))).toDF("i", "s", "arr")

    // Test cases with a primitive type
    val intDF = Seq(
      (Seq(Seq(1, 2, 3), Seq(4, 5), Seq(6))),
      (Seq(Seq(1, 2))),
      (Seq(Seq(1), Seq.empty)),
      (Seq(Seq.empty, Seq(1))),
      (Seq(Seq.empty, Seq.empty)),
      (Seq(Seq(1), null)),
      (Seq(null, Seq(1))),
      (Seq(null, null))
    ).toDF("i")

    val intDFResult = Seq(
      Row(Seq(1, 2, 3, 4, 5, 6)),
      Row(Seq(1, 2)),
      Row(Seq(1)),
      Row(Seq(1)),
      Row(Seq.empty),
      Row(null),
      Row(null),
      Row(null))

    checkAnswer(intDF.select(flatten($"i")), intDFResult)
    checkAnswer(intDF.filter(dummyFilter($"i"))select(flatten($"i")), intDFResult)
    checkAnswer(intDF.selectExpr("flatten(i)"), intDFResult)
    checkAnswer(
      oneRowDF.selectExpr("flatten(array(arr, array(null, 5), array(6, null)))"),
      Seq(Row(Seq(1, 2, 3, null, 5, 6, null))))

    // Test cases with non-primitive types
    val strDF = Seq(
      (Seq(Seq("a", "b"), Seq("c"), Seq("d", "e", "f"))),
      (Seq(Seq("a", "b"))),
      (Seq(Seq("a", null), Seq(null, "b"), Seq(null, null))),
      (Seq(Seq("a"), Seq.empty)),
      (Seq(Seq.empty, Seq("a"))),
      (Seq(Seq.empty, Seq.empty)),
      (Seq(Seq("a"), null)),
      (Seq(null, Seq("a"))),
      (Seq(null, null))
    ).toDF("s")

    val strDFResult = Seq(
      Row(Seq("a", "b", "c", "d", "e", "f")),
      Row(Seq("a", "b")),
      Row(Seq("a", null, null, "b", null, null)),
      Row(Seq("a")),
      Row(Seq("a")),
      Row(Seq.empty),
      Row(null),
      Row(null),
      Row(null))

    checkAnswer(strDF.select(flatten($"s")), strDFResult)
    checkAnswer(strDF.filter(dummyFilter($"s")).select(flatten($"s")), strDFResult)
    checkAnswer(strDF.selectExpr("flatten(s)"), strDFResult)
    checkAnswer(
      oneRowDF.selectExpr("flatten(array(array(arr, arr), array(arr)))"),
      Seq(Row(Seq(Seq(1, 2, 3), Seq(1, 2, 3), Seq(1, 2, 3)))))

    // Error test cases
    intercept[AnalysisException] {
      oneRowDF.select(flatten($"arr"))
    }
    intercept[AnalysisException] {
      oneRowDF.select(flatten($"i"))
    }
    intercept[AnalysisException] {
      oneRowDF.select(flatten($"s"))
    }
    intercept[AnalysisException] {
      oneRowDF.selectExpr("flatten(null)")
    }
  }

  private def assertValuesDoNotChangeAfterCoalesceOrUnion(v: Column): Unit = {
    import DataFrameFunctionsSuite.CodegenFallbackExpr
    for ((codegenFallback, wholeStage) <- Seq((true, false), (false, false), (false, true))) {
      val c = if (codegenFallback) {
        Column(CodegenFallbackExpr(v.expr))
      } else {
        v
      }
      withSQLConf(
        (SQLConf.CODEGEN_FALLBACK.key, codegenFallback.toString),
        (SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, wholeStage.toString)) {
        val df = spark.range(0, 4, 1, 4).withColumn("c", c)
        val rows = df.collect()
        val rowsAfterCoalesce = df.coalesce(2).collect()
        assert(rows === rowsAfterCoalesce, "Values changed after coalesce when " +
          s"codegenFallback=$codegenFallback and wholeStage=$wholeStage.")

        val df1 = spark.range(0, 2, 1, 2).withColumn("c", c)
        val rows1 = df1.collect()
        val df2 = spark.range(2, 4, 1, 2).withColumn("c", c)
        val rows2 = df2.collect()
        val rowsAfterUnion = df1.union(df2).collect()
        assert(rowsAfterUnion === rows1 ++ rows2, "Values changed after union when " +
          s"codegenFallback=$codegenFallback and wholeStage=$wholeStage.")
      }
    }
  }

  test("SPARK-14393: values generated by non-deterministic functions shouldn't change after " +
    "coalesce or union") {
    Seq(
      monotonically_increasing_id(), spark_partition_id(),
      rand(Random.nextLong()), randn(Random.nextLong())
    ).foreach(assertValuesDoNotChangeAfterCoalesceOrUnion(_))
  }

  test("SPARK-21281 use string types by default if array and map have no argument") {
    val ds = spark.range(1)
    var expectedSchema = new StructType()
      .add("x", ArrayType(StringType, containsNull = false), nullable = false)
    assert(ds.select(array().as("x")).schema == expectedSchema)
    expectedSchema = new StructType()
      .add("x", MapType(StringType, StringType, valueContainsNull = false), nullable = false)
    assert(ds.select(map().as("x")).schema == expectedSchema)
  }

  test("SPARK-21281 fails if functions have no argument") {
    val df = Seq(1).toDF("a")

    val funcsMustHaveAtLeastOneArg =
      ("coalesce", (df: DataFrame) => df.select(coalesce())) ::
      ("coalesce", (df: DataFrame) => df.selectExpr("coalesce()")) ::
      ("named_struct", (df: DataFrame) => df.select(struct())) ::
      ("named_struct", (df: DataFrame) => df.selectExpr("named_struct()")) ::
      ("hash", (df: DataFrame) => df.select(hash())) ::
      ("hash", (df: DataFrame) => df.selectExpr("hash()")) :: Nil
    funcsMustHaveAtLeastOneArg.foreach { case (name, func) =>
      val errMsg = intercept[AnalysisException] { func(df) }.getMessage
      assert(errMsg.contains(s"input to function $name requires at least one argument"))
    }

    val funcsMustHaveAtLeastTwoArgs =
      ("greatest", (df: DataFrame) => df.select(greatest())) ::
      ("greatest", (df: DataFrame) => df.selectExpr("greatest()")) ::
      ("least", (df: DataFrame) => df.select(least())) ::
      ("least", (df: DataFrame) => df.selectExpr("least()")) :: Nil
    funcsMustHaveAtLeastTwoArgs.foreach { case (name, func) =>
      val errMsg = intercept[AnalysisException] { func(df) }.getMessage
      assert(errMsg.contains(s"input to function $name requires at least two arguments"))
    }
  }
}

object DataFrameFunctionsSuite {
  case class CodegenFallbackExpr(child: Expression) extends Expression with CodegenFallback {
    override def children: Seq[Expression] = Seq(child)
    override def nullable: Boolean = child.nullable
    override def dataType: DataType = child.dataType
    override lazy val resolved = true
    override def eval(input: InternalRow): Any = child.eval(input)
  }
}
