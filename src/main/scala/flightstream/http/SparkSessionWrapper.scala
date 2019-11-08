package flightstream.http

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  implicit val spark: SparkSession = {
    val spark = SparkSession.builder.master("local[*]").appName("Flight Stream").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  def stopSpark(): Unit = spark.stop()

}
