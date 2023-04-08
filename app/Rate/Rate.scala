package Rate

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import utils.Utils.{dataFrameToJson, getData}

import java.nio.file.{Files, Paths}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.Source.fromFile

object Rate extends {

  val siteName = "https://www.cnb.cz/en/financial_markets/foreign_exchange_market/exchange_rate_fixing/daily.txt?date="

  def getDataForPeriod(startDate: String, endDate: String): DataFrame = {

    val spark = SparkSession
      .builder()
      .appName(s"sdvs")
      .master("local[*]")
      .getOrCreate()

    val dateStart = LocalDate.parse(startDate, DateTimeFormatter.ofPattern("dd-MM-yyyy"))
    val dateEnd = LocalDate.parse(endDate, DateTimeFormatter.ofPattern("dd-MM-yyyy"))


    spark.conf.set("spark.sql.caseSensitive", "false")
    val saa = between(dateStart, dateEnd)

    val schema = StructType(
      StructField("Country", StringType, true) ::
        StructField("Currency", StringType, true) ::
        StructField("Amount", StringType, true) ::
        StructField("Code", StringType, true) ::
        StructField("Rate", StringType, true) ::
        StructField("Date", StringType, true) :: Nil
    )

    var allData = spark.createDataFrame(spark.sparkContext
      .emptyRDD[Row], schema)

    saa.foreach(x => {
      val dateForGetData = x.format(DateTimeFormatter.ofPattern("dd-MM-yyyy"))
      val data = getData(siteName, dateForGetData)
      val processedData = data.substring(data.indexOf("\n") + 1)

      Files.write(Paths.get("temp.txt"), processedData.getBytes())

      val df = spark.read.options(Map("header" -> "true", "delimiter" -> "|")).csv("temp.txt").withColumn("Date", lit(dateForGetData))
      allData = allData.unionByName(df)
    })

    allData
  }

  def getDataForApi(startDate: String, endDate: String): String = {

    val data = getDataForPeriod(startDate, endDate)

    val source = fromFile("currency.txt") //Считывание полученного файла
    val currencysString = try source.mkString.split(",") finally source.close()
    val currencysSeq = currencysString.toSeq

    val list = List("EMU euro", "Canada dollar", "Hongkong dollar")
    currencysSeq.foreach(println(_))
    val df = data
      .withColumn("Currency_Country", functions.concat_ws(" ", col("Country"), col("Currency")))
      .filter(col("Currency_Country").isin(currencysSeq:_*))
      .withColumn("Amount_Rate",
        col("Rate").cast("decimal(38,3)") / col("Amount").cast("decimal(38,3)"))
      .groupBy(col("Currency_Country"))
      .agg(max("Amount_Rate") as "maxRate",
        min("Amount_Rate") as "minRate",
        avg("Amount_Rate") as "avgRate")

    df.show()
    dataFrameToJson(df)
  }


  def between(fromDate: LocalDate, toDate: LocalDate) = {
    fromDate.toEpochDay.to(toDate.toEpochDay).map(LocalDate.ofEpochDay)
  }
}
