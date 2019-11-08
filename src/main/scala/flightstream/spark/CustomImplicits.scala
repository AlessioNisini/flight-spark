package flightstream.spark

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object CustomImplicits {

  class RemovePrefix(col: Column, separator: Char) {
    def removePrefix: Column =
      if(col.toString.contains(separator))
        col.as(col.toString.dropWhile(_ != separator).drop(1))
      else
        col
  }

  class AddPrefix(df: DataFrame, separator: Char) {
    def addPrefix(prefix: String): DataFrame =
      df.toDF(df.columns.map(x => s"$prefix$separator$x"): _*)
  }

  class ReadAndLoadJson(spark: SparkSession) {
    def readAndLoadJson(path: String): DataFrame =
      spark.read.format("json").option("multiLine", true).load(path)
  }

  implicit def removeColumnPrefix(col: Column): RemovePrefix = new RemovePrefix(col, '_')
  implicit def addDataFramePrefix(df: DataFrame): AddPrefix = new AddPrefix(df, '_')
  implicit def loadJson(spark: SparkSession): ReadAndLoadJson = new ReadAndLoadJson(spark)

}
