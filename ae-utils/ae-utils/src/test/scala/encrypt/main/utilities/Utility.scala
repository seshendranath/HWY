package com.homeaway.analyticsengineering.encrypt.main.utilities


import com.homeaway.analytics.clickstream.utils.SparkTestBase
import org.scalatest.FlatSpec
import org.scalatest._


class UtilityForTest extends Utility{}

class UtilityTest extends FlatSpec with Matchers with SparkTestBase{
  var util: UtilityForTest = null

  override def beforeAll: Unit = {
    super.beforeAll
    util = new UtilityForTest()
  }

  "constructHivePartitions" should "work the same with the old and new logic for simple cases" in {
    val df = this.sparkSession.createDataFrame(Seq(
      (1, "2017-01-01"),
      (2, "2016-02-02")
    )).selectExpr("_1 as a", "_2 as b")
    val basePath = "s3://base/path"
    val oldOutput = util.deprecatedConstructHivePartitions(df, Seq("a"), basePath)
    val newOutput = util.constructHivePartitions(df, Seq("a"), basePath)

    val expectedOutput = Seq(
      (Seq("1"), "s3://base/path/a=1"),
      (Seq("2"), "s3://base/path/a=2")
    )

    assert(oldOutput == expectedOutput)
    assert(newOutput == expectedOutput)
  }

  "constructHivePartitions" should "handle base locations that end with a slash correctly" in {
    val df = this.sparkSession.createDataFrame(Seq(
      (1, "2017-01-01"),
      (2, "2016-02-02")
    )).selectExpr("_1 as a", "_2 as b")
    val basePath = "s3://base/path/"
    val oldOutput = util.deprecatedConstructHivePartitions(df, Seq("a"), basePath)
    val newOutput = util.constructHivePartitions(df, Seq("a"), basePath)

    val expectedOutput = Seq(
      (Seq("1"), "s3://base/path/a=1"),
      (Seq("2"), "s3://base/path/a=2")
    )

    assert(oldOutput != expectedOutput)
    assert(newOutput == expectedOutput)
  }

  "constructHivePartitions" should "only work correctly with the new logic when there are special characters" in {
    val df = this.sparkSession.createDataFrame(Seq(
      (1, "2017/01/01", "abc"),
      (2, "2016/02/02", "def")
    )).selectExpr("_1 as a", "_2 as b", "_3 as c")
    val basePath = "s3://base/path"
    val partCols = Seq("a", "b")
    val oldOutput = util.deprecatedConstructHivePartitions(df, partCols, basePath)
    val newOutput = util.constructHivePartitions(df, partCols, basePath)

    val expectedOutput = Seq(
      (Seq("1", "2017/01/01"), "s3://base/path/a=1/b=2017%2F01%2F01"),
      (Seq("2", "2016/02/02"), "s3://base/path/a=2/b=2016%2F02%2F02")
    )

    // toSet to handle unorderedness of dataframes
    assert(oldOutput.toSet != expectedOutput.toSet)
    assert(newOutput.toSet == expectedOutput.toSet)
  }
}