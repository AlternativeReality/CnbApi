package utils

import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.Utils.writeIntoPostgres

import java.time.LocalDateTime

class Log(
           spark: SparkSession,
           logTable: String
         ) {
  private var logs: Seq[(String, String)] = Seq()

  def WriteLogs: Unit = {
    if (logs.nonEmpty) {
      print(logs)
      val df = spark.createDataFrame(logs)
        .toDF("processed_dttm", "query")
        .withColumn("processed_dttm", to_date(col("processed_dttm")))

      writeIntoPostgres(df, logTable)
    }
  }

  def error(query: String): Unit = {
    this.logs = Seq((LocalDateTime.now().toString, query))
    WriteLogs
  }

  def warning(query: String): Unit = {
    this.logs = Seq((LocalDateTime.now().toString, query))
    WriteLogs
  }

  def log(query: String): Unit = {
    this.logs = Seq((LocalDateTime.now().toString, query))
    WriteLogs
  }

  def success(query: String): Unit = {
    this.logs = Seq((LocalDateTime.now().toString, query))
    WriteLogs
  }

}
