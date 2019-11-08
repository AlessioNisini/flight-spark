package flightstream.spark

import flightstream.model._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.types.IntegerType

object Aggregator extends SparkSessionWrapper {

  import spark.implicits._

  def getTotalFlight(flightReceived: DataFrame): OutputMessage = {
    TotalFlight(
      flightReceived.count()
    )
  }

  def getTotalAirline(flightReceived: DataFrame): OutputMessage = {
    TotalAirline(
      flightReceived
        .select(countDistinct($"airline.codeAirline"))
        .first()
        .getLong(0)
    )
  }

  def getTopDeparture(flightReceived: DataFrame, n: Int): List[OutputMessage] = {
    flightReceived
      .groupBy($"airportDeparture.codeAirport".as("code"))
      .count()
      .sort($"count".desc)
      .limit(n)
      .as[TopDeparture]
      .collect()
      .toList
  }

  def getTopArrival(flightReceived: DataFrame, n: Int): List[OutputMessage] = {
    flightReceived
      .groupBy($"airportArrival.codeAirport".as("code"))
      .count()
      .sort($"count".desc)
      .limit(n)
      .as[TopArrival]
      .collect()
      .toList
  }

  def getTopAirline(flightReceived: DataFrame, n: Int): List[OutputMessage] = {
    flightReceived
      .groupBy($"airline.nameAirline".as("name"))
      .count()
      .sort($"count".desc)
      .limit(n)
      .as[TopAirline]
      .collect()
      .toList
  }

  def getTopSpeed(flightReceived: DataFrame, n: Int): List[OutputMessage] = {
    flightReceived
      .select($"icaoNumber".as("code"), $"speed".cast(IntegerType))
      .sort($"speed".desc)
      .limit(n)
      .as[TopSpeed]
      .collect()
      .toList
  }

}
