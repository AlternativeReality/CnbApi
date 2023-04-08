package db_controller

import Rate.Rate.getDataForPeriod
import org.apache.spark.sql.catalyst.expressions.Log
import org.apache.spark.sql.functions.{max, to_date}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.Utils
import utils.Utils.{dataFrameToJson, getTable, writeIntoPostgres}

object DbController {
  val spark: SparkSession = SparkSession.builder()
    .appName("Example1")
    .master("local[*]")
    .getOrCreate()

  val logger = new utils.Log(spark, "query_log")

  import spark.implicits._



  def writeRatesIntoDb(startDate: String, endDate: String): Boolean = {
    val newData = getDataForPeriod(startDate, endDate)
    writeIntoPostgres(newData, "rates")
    return true
  }


}
