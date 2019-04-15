package com.jakainas

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import com.jakainas.table.Table
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, Dataset, Encoder, SparkSession, _}

import scala.reflect.runtime.universe._

package object functions {
  /**
   * Returns a default value for a given column if it's null, otherwise return the column's value
   * @param c - Column to be checked
   * @param default - Value to return if `c` is null
   * @return c if not null and default otherwise
   */
  def nvl[C1](c: C1, default: Any)(implicit ev: C1 => Column): Column = when(c.isNotNull, c).otherwise(default)

  /**
   * Returns the schema for a type (e.g. case class, string, etc)
   * @tparam T - The type whose schema is to be returned
   * @return The schema for a Dataset built with that type
   */
  def schemaFor[T: TypeTag]: StructType = {
    val dataType = ScalaReflection.schemaFor[T].dataType
    dataType.asInstanceOf[StructType]
  }

  /**
   * Adds days to a given date
   * @param date - date to be added to in the format of '2019-01-20'
   * @param numDays - number of days to add, can be negative
   * @return numDays after (or before if negative) `date`
   */
  def plusDays(date: String, numDays: Int): String = {
    LocalDate.parse(date, DateTimeFormatter.ISO_DATE).minusDays(-numDays).toString
  }

  /**
   * Returns a list of dates that lie between two given dates
   *
   * @param start - start date (yyyy-mm-dd)
   * @param end   - end date (yyyy-mm-dd)
   * @return The dates between start and end in the form of a sequence of strings
   */
  def dateRange(start: String, end: String): IndexedSeq[String] = {
    val days = ChronoUnit.DAYS.between(LocalDate.parse(start, DateTimeFormatter.ISO_DATE), LocalDate.parse(end, DateTimeFormatter.ISO_DATE)).toInt
    require(days >= 0, s"Start date ($start) must be before end date ($end)!")
    (0 to days).map(d => plusDays(start, d))
  }

  /**
   * Today as a string in UTC
   * @return Today's date in UTC(String)
   */
  def today: String = LocalDate.now().toString

  /**
   * Yesterday as a string in UTC
   * @return Yesterday's date in UTC(String)
   */
  def yesterday: String = LocalDate.now().minusDays(1).toString

  /**
   * Generate a string Date column by combining year, month, and day columns.
   * If one or more column is null, the result Date will be null.
   * @param year - the year column
   * @param month - the month column
   * @param day - the day column
   * @return a string column consist of the combination of year, month, and day into date
   */
  def to_date_str(year: Column, month: Column, day: Column): Column = {
    date_format(concat(year, lit("-"), month, lit("-"), day), "yyyy-MM-dd")
  }

  /**
    * Convert a date into a list of form (year,month,day)
    * @param date - date in the form of a string('yyyy-mm-dd')
    * @return a list containing the year, month and day of the date
    */
  def expandDate(date: String) = {
    val d = LocalDate.parse(date, DateTimeFormatter.ISO_DATE)
    val year = d.getYear
    val month = d.getMonthValue
    val day = d.getDayOfMonth
    List(year, month, day)
  }

  /**
    * Convert a dateRange into its corresponding SQL query
    * dateRangeToSql("2017-01-09", "2019-04-10") returns: "(year = 2017 and month > 1) or (year = 2017 and month = 1 and day >= 9) or year = 2018 or (year = 2019 and ((month < 4) or (month = 4 and day <= 10)))"
    * @param date1 - some date ('yyyy-mm-dd')
    * @param date2 - some date ('yyyy-mm-dd')
    * @return SQL query in string form
    */
  def dateRangeToSql(date1: String, date2: String) = {
    val dateFormat = new SimpleDateFormat("yyyy-mm-dd")
    val check = dateFormat.parse(date1).compareTo(dateFormat.parse(date2))
    var start = List[Int]()
    var end = List[Int]()
    val yearsDiff = ChronoUnit.YEARS.between(LocalDate.parse(date1, DateTimeFormatter.ISO_DATE), LocalDate.parse(date2, DateTimeFormatter.ISO_DATE)).toInt

    if (check != 0) {
      if (check > 0) {
        start = expandDate(date2)
        end = expandDate(date1)
      }
      else {
        start = expandDate(date1)
        end = expandDate(date2)
      }
      val years = (1 to (yearsDiff - 1)).map(y => start(0) + y).toList
      s"(year = ${start(0)} and month > ${start(1)}) or (year = ${start(0)} and month = ${start(1)} and day >= ${start(2)}) or year ${if (years.length < 2) s"= ${years.head}" else s"in (${years.mkString(",")})"} or (year = ${end(0)} and ((month < ${end(1)}) or (month = ${end(1)} and day <= ${end(2)})))"
    }
    else {
      start = expandDate(date2)
      s"year = ${start(0)} and month = ${start(1)} and day = ${start(2)}"
    }
  }

  implicit class DatasetFunctions[T](private val ds: Dataset[T]) extends AnyVal {
    /**
     * Remove duplicate rows using some column criteria for grouping and ordering
     * @param partCols - How to group rows.  Only 1 row from each group will be in the result
     * @param order - How to sort rows.  The row with the first order position will be kept
     * @param conv - Optional: Encoder for T, allowing us to work with both Dataset and DataFrames
     * @return The data with only 1 row per group based on given sort order
     */
    def distinctRows(partCols: Seq[Column], order: Seq[Column])(implicit conv: Encoder[T] = null): Dataset[T] = {
      var i = 0
      var colName = s"rn$i"

      while (ds.columns.contains(colName)) {
        i += 1
        colName = s"rn$i"
      }

      val deduped = ds.withColumn(s"rn$i", row_number over Window.partitionBy(partCols: _*)
        .orderBy(order: _*)).where(col(colName) === 1).drop(colName)

      if (conv == null)
        deduped.asInstanceOf[Dataset[T]]
      else deduped.as[T](conv)
    }

    /**
     * Add multiple new columns to the current DataFrame
     * (i.e., `withColumn` for a sequence of (String, Column) Tuples).
     *
     * @param newColTuples - a list of name-value Tuples2 (colName: String, colVal: Column).
     * @return - The DataFrame with new columns added.
     */
    def addColumns(newColTuples: (String, Column)*): DataFrame = newColTuples.foldLeft(ds.toDF()) {
      // From left to right, for each new (colName, colVal) Tuple add it to the current DataFrame
      case (newDF, (colName, colVal)) => newDF.withColumn(colName, colVal)
    }

    /**
     * Rename multiple new columns of the current DataFrame
     * (i.e., `withColumnRenamed` for a sequence of (String, String) Tuples).
     *
     * @param renameColTuples - a list of current, new column name Tuples2 (currColName: String, newColName: String).
     * @return - The DataFrame with mulitple renamed columns.
     */
    def renameColumns(renameColTuples: (String, String)*): DataFrame = renameColTuples.foldLeft(ds.toDF()) {
      // From left to right, for each new (currColName, newColName) Tuple apply withColumnRenamed
      case (newDF, (currColName, newColName)) => newDF.withColumnRenamed(currColName, newColName)
    }

    def save()(implicit table: Table[T]): Unit = {
      ds.write.partitionBy(table.partitioning: _*).parquet(table.fullPath)
    }
  }

  implicit class SparkFunctions(val spark: SparkSession) extends AnyVal {
    def load[T: Encoder: Table]: Dataset[T] = {
      val table = implicitly[Table[T]]

      spark.read.option("basePath", table.basePath).parquet(table.fullPath).as[T]
    }
  }
}
