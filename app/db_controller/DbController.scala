package db_controller

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

  def getAllPersonsFromDb(): String = {
    logger.log("get all persons from DB")
    getTable("corporate_name", spark).toJSON.collect().mkString("[", ",", "]")
  }

  def getPersonByIdFromDb(id: Integer): String = {
    logger.log("get person by id")

    val df = getTable("corporate_name", spark).filter($"id" === s"$id")
    dataFrameToJson(df)
  }

  def getAllWorkPlacesFromDb(): String = {
    logger.log("get all work places from DB")

    val df = getTable("work_places", spark)
    dataFrameToJson(df)
  }

  def getAllDataFromDb(): String = {
    logger.log("get all data from DB")

    val dfPersons = getTable("corporate_name", spark)
    val dfPersonWork = getTable("corporate_work_place", spark)
    val dfWorkPlaces = getTable("work_places", spark)
    val dfBankAccount = getTable("corporate_bank_account", spark)

    val finalDf = dfPersons.join(dfPersonWork, Seq("id"))
      .join(dfWorkPlaces, Seq("work_id"))
      .join(dfBankAccount, dfPersons("id") === dfBankAccount("owner_id"))

    dataFrameToJson(finalDf)
  }

  def addNewPerson(name: String): Unit = {
    val columns = Seq("id", "name")
    val data0 = Seq(
      (s"", s"$name")
    )
    val newFrame = spark.createDataFrame(data0).toDF(columns: _*)
    newFrame.show()
    writeIntoPostgres(newFrame, "corporate_name")
  }
}
