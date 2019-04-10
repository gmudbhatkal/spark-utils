package com.jakainas.functions

import com.jakainas.functions.functionsTest.TestData

class functionsTest extends SparkTest {
  import org.apache.spark.sql.functions._
  import spark.implicits._

  test("distinct by removes duplicates") {
    // with a dataset
    Seq("a", "b", "a").toDS().distinctRows(Seq('value), Seq(lit(true)))
      .as[String].collect should contain theSameElementsAs Array("a", "b")

    // with a dataframe
    Seq("a", "b", "a").toDF("value").distinctRows(Seq('value), Seq(lit(true)))
      .collect().map(_.getString(0)) should contain theSameElementsAs Array("a", "b")

    // multi-column case class
    Seq(TestData("a", 7), TestData("b", 3), TestData("a", 2)).toDS
      .distinctRows(Seq('x), Seq('y.desc))
      .collect() should contain theSameElementsAs Array(TestData("a", 7), TestData("b", 3))
  }

  test("nvl replaces null") {
    Seq("a", "b", null).toDS.select(nvl('value, "c"))
      .as[String].collect should contain theSameElementsAs Array("a", "b", "c")

    Seq(("a", "a1"), (null, "b1")).toDF("a", "b").withColumn("a", nvl('a, 'b))
      .as[(String, String)].collect should contain theSameElementsAs Array(("a", "a1"), ("b1", "b1"))
  }

  test("can generate the schema of a case class") {
    val schema = Seq(TestData("a", 7), TestData("b", 3)).toDS.schema

    schemaFor[TestData] shouldEqual schema
  }

  test("add days to a given date") {
    plusDays("2018-01-10", 5) shouldEqual "2018-01-15"
    plusDays("2018-01-10", -5) shouldEqual "2018-01-05"
    plusDays("2018-01-10", 0) shouldEqual "2018-01-10"
    an [NullPointerException] should be thrownBy plusDays(null, 5)
  }

  test("return a list of dates between two given dates") {
    dateRange("2018-01-10","2018-01-10") shouldEqual Seq("2018-01-10")
    dateRange("2018-01-10","2018-01-14") shouldEqual Seq("2018-01-10","2018-01-11","2018-01-12","2018-01-13","2018-01-14")
    dateRange("2018-01-30", "2018-02-04") shouldEqual Seq("2018-01-30","2018-01-31","2018-02-01","2018-02-02","2018-02-03","2018-02-04")
    dateRange("2018-12-25", "2019-01-05") shouldEqual Seq("2018-12-25","2018-12-26","2018-12-27","2018-12-28","2018-12-29","2018-12-30","2018-12-31","2019-01-01","2019-01-02","2019-01-03","2019-01-04","2019-01-05")
    an [IllegalArgumentException] should be thrownBy dateRange("2018-01-14", "2018-01-10")
  }

}

object functionsTest {
  case class TestData(x: String, y: Int)
}
