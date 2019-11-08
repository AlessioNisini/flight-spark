package flightstream.spark

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  lazy val spark: SparkSession = {
    val spark = SparkSession.builder.master("local[*]").appName("Flight Stream").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

}
