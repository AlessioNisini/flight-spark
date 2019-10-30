package flightstream.spark

import flightstream.model._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{countDistinct, regexp_replace, struct}
import org.apache.spark.sql.types.IntegerType

object FlightStream {

  def init(): SparkSession = {
    val spark = SparkSession.builder.master("local[*]").appName("Flight Stream").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  def run(req: FlightRequest)(implicit spark: SparkSession): OutputMessage = {
    val flightReceived = buildFlightReceived(spark)
    val result = req match {
      case FlightRequest(source, _) if source == "totalFlight" => getTotalFlight(flightReceived)
      case FlightRequest(source, _) if source == "totalAirline" => getTotalAirline(flightReceived)
      case FlightRequest(source, limit) if source == "topDeparture" => getTopDeparture(flightReceived, limit)
      case FlightRequest(source, limit) if source == "topArrival" => getTopArrival(flightReceived, limit)
      case FlightRequest(source, limit) if source == "topAirline" => getTopAirline(flightReceived, limit)
      case FlightRequest(source, limit) if source == "topSpeed" => getTopSpeed(flightReceived, limit)
    }
    result
  }

  def buildFlightReceived(spark: SparkSession): DataFrame = {

    import spark.implicits._
    import CustomImplicits._

    val flight = spark.readAndLoadJson("flights.json")
    val airport = spark.readAndLoadJson("airportDatabase.json")
    val airline = spark.readAndLoadJson("airlineDatabase.json")
    val airplane = spark.readAndLoadJson("airplaneDatabase.json")

    val flightFiltered = flight.filter($"status" === "en-route" && !($"departure.iataCode" === "" || $"arrival.iataCode" === ""))
    val flightPrefixed = flightFiltered.addPrefix("FLIGHT")

    val departureAirportPrefixed = airport.addPrefix("DEPARTURE")
    val arrivalAirportPrefixed = airport.addPrefix("ARRIVAL")

    val airlineFiltered = airline.filter($"statusAirline" === "active")
    val airlinePrefixed = airlineFiltered.addPrefix("AIRLINE")

    val airplaneUpdated = airplane.withColumn("numberRegistration", regexp_replace($"numberRegistration", "-", ""))
    val airplanePrefixed = airplaneUpdated.addPrefix("AIRPLANE")

    val bigFlights =
      flightPrefixed
        .join(departureAirportPrefixed, $"FLIGHT_departure.iatacode" === $"DEPARTURE_codeIataAirport")
        .join(arrivalAirportPrefixed, $"FLIGHT_arrival.iatacode" === $"ARRIVAL_codeIataAirport")
        .join(airlinePrefixed, $"FLIGHT_airline.icaocode" === $"AIRLINE_codeIcaoAirline")
        .join(airplanePrefixed, $"FLIGHT_aircraft.regNumber" === $"AIRPLANE_numberRegistration")

    val flightReceived = bigFlights.select(
      $"FLIGHT_flight.iataNumber".as("iataNumber"),
      $"FLIGHT_flight.icaoNumber".as("icaoNumber"),
      $"FLIGHT_geography".removePrefix,
      $"FLIGHT_speed.horizontal".as("speed"),
      struct(
        $"DEPARTURE_codeIcaoAirport".as("codeAirport"),
        $"DEPARTURE_nameAirport".removePrefix,
        $"DEPARTURE_nameCountry".removePrefix,
        $"DEPARTURE_codeIso2Country".removePrefix,
        $"DEPARTURE_timezone".removePrefix,
        $"DEPARTURE_GMT".removePrefix,
      ).as("airportDeparture"),
      struct(
        $"ARRIVAL_codeIcaoAirport".as("codeAirport"),
        $"ARRIVAL_nameAirport".removePrefix,
        $"ARRIVAL_nameCountry".removePrefix,
        $"ARRIVAL_codeIso2Country".removePrefix,
        $"ARRIVAL_timezone".removePrefix,
        $"ARRIVAL_GMT".removePrefix,
      ).as("airportArrival"),
      struct(
        $"AIRLINE_codeIcaoAirline".as("codeAirline"),
        $"AIRLINE_nameAirline".removePrefix,
        $"AIRLINE_sizeAirline".removePrefix
      ).as("airline"),
      struct(
        $"AIRPLANE_numberRegistration".removePrefix,
        $"AIRPLANE_productionLine".removePrefix,
        $"AIRPLANE_modelCode".removePrefix
      ).as("airplane"),
      $"FLIGHT_status".removePrefix,
      $"FLIGHT_system.updated".as("updated")
    )

    flightReceived.cache()

    flightReceived

  }

  def getTotalFlight(flightReceived: DataFrame): OutputMessage = {
    OutputMessage(List(TotalFlight(flightReceived.count())))
  }

  def getTotalAirline(flightReceived: DataFrame)(implicit spark: SparkSession): OutputMessage = {
    import spark.implicits._
    val result = flightReceived
      .select(countDistinct($"airline.codeAirline"))
      .first()
      .getLong(0)
    OutputMessage(List(TotalAirline(result)))
  }

  def getTopDeparture(flightReceived: DataFrame, n: Int)(implicit spark: SparkSession): OutputMessage = {
    import spark.implicits._
    val list = flightReceived
      .groupBy($"airportDeparture.codeAirport".as("code"))
      .count()
      .sort($"count".desc)
      .limit(n)
      .as[TopDeparture]
      .collect()
      .toList
    OutputMessage(list)
  }

  def getTopArrival(flightReceived: DataFrame, n: Int)(implicit spark: SparkSession): OutputMessage = {
    import spark.implicits._
    val list = flightReceived
      .groupBy($"airportArrival.codeAirport".as("code"))
      .count()
      .sort($"count".desc)
      .limit(n)
      .as[TopArrival]
      .collect()
      .toList
    OutputMessage(list)
  }

  def getTopAirline(flightReceived: DataFrame, n: Int)(implicit spark: SparkSession): OutputMessage = {
    import spark.implicits._
    val list = flightReceived
      .groupBy($"airline.nameAirline".as("code"))
      .count()
      .sort($"count".desc)
      .limit(n)
      .as[TopAirline]
      .collect()
      .toList
    OutputMessage(list)
  }

  def getTopSpeed(flightReceived: DataFrame, n: Int)(implicit spark: SparkSession): OutputMessage = {
    import spark.implicits._
    val list = flightReceived
      .select(
        $"icaoNumber".as("code"),
        $"speed".cast(IntegerType).as("count")
      )
      .sort($"count".desc)
      .limit(n)
      .as[TopSpeed]
      .collect()
      .toList
    OutputMessage(list)
  }

}
