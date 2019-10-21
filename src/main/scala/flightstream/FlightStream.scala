package flightstream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FlightStream extends App {

  val spark = SparkSession.builder.master("local[*]").appName("Simple Application").getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._
  import CustomImplicits._

  val flight = spark.readAndLoadJson("flights.json")
  val flightFiltered = flight.filter($"status" === "en-route" && !($"departure.iataCode" === "" || $"arrival.iataCode" === ""))
  val flightPrefixed = flightFiltered.addPrefix("FLIGHT")

  val airport = spark.readAndLoadJson("airportDatabase.json")
  val departureAirportPrefixed = airport.addPrefix("DEPARTURE")
  val arrivalAirportPrefixed = airport.addPrefix("ARRIVAL")

  val airline = spark.readAndLoadJson("airlineDatabase.json")
  val airlineFiltered = airline.filter($"statusAirline" === "active")
  val airlinePrefixed = airlineFiltered.addPrefix("AIRLINE")

  val airplane = spark.readAndLoadJson("airplaneDatabase.json")
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
      $"DEPARTURE_codeIcaoAirport".removePrefix,
      $"DEPARTURE_nameAirport".removePrefix,
      $"DEPARTURE_nameCountry".removePrefix,
      $"DEPARTURE_codeIso2Country".removePrefix,
      $"DEPARTURE_timezone".removePrefix,
      $"DEPARTURE_GMT".removePrefix,
    ).as("airportDeparture"),
    struct(
      $"ARRIVAL_codeIcaoAirport".removePrefix,
      $"ARRIVAL_nameAirport".removePrefix,
      $"ARRIVAL_nameCountry".removePrefix,
      $"ARRIVAL_codeIso2Country".removePrefix,
      $"ARRIVAL_timezone".removePrefix,
      $"ARRIVAL_GMT".removePrefix,
    ).as("airportArrival"),
    struct(
      $"AIRLINE_codeIcaoAirline".removePrefix,
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

  flightReceived.printSchema()
  flightReceived.show()

  spark.stop()

}
