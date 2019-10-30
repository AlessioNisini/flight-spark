package flightstream.spark

import flightstream.model._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{countDistinct, regexp_replace, struct}
import org.apache.spark.sql.types.IntegerType

object FlightStream {

  val spark: SparkSession = SparkSession.builder.master("local[*]").appName("Flight Stream").getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", "5")
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val flightReceived: DataFrame = buildFlightReceived

  def getTotalFlight: OutputMessage = {
    TotalFlight(flightReceived.count())
  }

  def getTotalAirline: OutputMessage = {
    val result = flightReceived
      .select(countDistinct($"airline.codeAirline"))
      .first()
      .getLong(0)
    TotalAirline(result)
  }

  def getTopDeparture(n: Int): List[OutputMessage] = {
    flightReceived
      .groupBy($"airportDeparture.codeAirport".as("code"))
      .count()
      .sort($"count".desc)
      .limit(n)
      .as[TopDeparture]
      .collect()
      .toList
  }

  def getTopArrival(n: Int): List[OutputMessage] = {
    flightReceived
      .groupBy($"airportArrival.codeAirport".as("code"))
      .count()
      .sort($"count".desc)
      .limit(n)
      .as[TopArrival]
      .collect()
      .toList
  }

  def getTopAirline(n: Int): List[OutputMessage] = {
    flightReceived
      .groupBy($"airline.nameAirline".as("name"))
      .count()
      .sort($"count".desc)
      .limit(n)
      .as[TopAirline]
      .collect()
      .toList
  }

  def getTopSpeed(n: Int): List[OutputMessage] = {
    flightReceived
      .select($"icaoNumber".as("code"), $"speed".cast(IntegerType))
      .sort($"speed".desc)
      .limit(n)
      .as[TopSpeed]
      .collect()
      .toList
  }

  private def buildFlightReceived: DataFrame = {

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

}
