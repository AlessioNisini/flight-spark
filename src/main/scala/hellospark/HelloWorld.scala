package hellospark

import org.apache.spark.sql.{Column, DataFrame, Dataset, SQLContext, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, Metadata, StringType, StructField, StructType}

object DataFrameCSV extends App {

  val spark = SparkSession.builder.master("local[*]").appName("Simple Application").getOrCreate()

  val flightData2015: DataFrame = spark.read.option("inferSchema", "true").option("header", "true").csv("src/main/resources/2015-summary.csv")

    flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort(desc("destination_total"))
      .limit(5)
      .show()

  spark.stop()

}

object DataSetParquet extends App {

  val spark = SparkSession.builder.master("local[*]").appName("Simple Application").getOrCreate()

  import spark.implicits._

  case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: Long)

  val flightDataFrame: DataFrame = spark.read.parquet("src/main/resources/2010-summary.parquet")
  val flight: Dataset[Flight] = flightDataFrame.as[Flight]

  flight.filter(_.DEST_COUNTRY_NAME == "United States").show()

  spark.stop()

}

object PrintSchemaJson extends App {

  val spark = SparkSession.builder.master("local[*]").appName("Simple Application").getOrCreate()

  val df: DataFrame = spark.read.format("json").load("src/main/resources/2015-summary.json")

  println(df.schema)

  val myManualSchema = StructType(
    Array(
      StructField("DEST_COUNTRY_NAME", StringType, true),
      StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      StructField("count", LongType, false, Metadata.fromJson("{\"hello\":\"world\"}"))
    )
  )

  val df2: DataFrame = spark.read.format("json").schema(myManualSchema).load("src/main/resources/2015-summary.json")

  println(df2.columns.toList)

  spark.stop()

}
