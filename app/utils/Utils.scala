package utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import requests.Response
object Utils {

  def getTable(name: String, spark: SparkSession, dbName: String = "db_for_api"): DataFrame ={
    spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", s"jdbc:postgresql://localhost:5432/$dbName")
      .option("dbtable", s"$name")
      .option("user", "postgres")
      .option("password", "admin")
      .load()
  }

  def dataFrameToJson(df: DataFrame): String ={
    df.toJSON.collect().mkString("[", ",", "]")
  }

  def writeIntoPostgres(newFrame: DataFrame, tableName: String, dbName: String = "db_for_api"): Unit = {
    newFrame.write
      .format("jdbc")
      .mode("overwrite")
      .option("driver", "org.postgresql.Driver")
      .option("url", s"jdbc:postgresql://localhost:5432/$dbName")
      .option("dbtable", tableName)
      .option("user", "postgres")
      .option("password", "admin")
      .save()
  }

  def getData(siteName: String, route: String): String = {
    val r = requests.get(
      siteName + route,
      check = false,
      readTimeout = 200000,
      connectTimeout = 200000,
      verifySslCerts = false
    )
    r.text()
  }

  def sendData(data: String, siteName: String, route: String): Response = {
    val r = requests.post(
      siteName + route,
      check = false,
      verifySslCerts = false,
      readTimeout = 200000,
      connectTimeout = 200000,
      headers = Map("accept" -> "text/plain", "Content-Type" -> "application/json-patch+json"),
      data = data)
    r
  }
}
