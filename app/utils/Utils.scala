package utils

import org.apache.spark.sql.{DataFrame, SparkSession}

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
      .mode("append")
      .option("driver", "org.postgresql.Driver")
      .option("url", s"jdbc:postgresql://localhost:5432/$dbName")
      .option("dbtable", tableName)
      .option("user", "postgres")
      .option("password", "admin")
      .save()
  }
}
