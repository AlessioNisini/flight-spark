package flightstream

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object FlightStream extends App {

  val spark = SparkSession.builder.master("local[*]").appName("Simple Application").getOrCreate()

  import spark.implicits._

  val flight = spark.read.format("json").option("multiLine", true).load("src/main/resources/flightstream/flights.json")
  val flightFiltered = flight.filter($"status" === "en-route" && !($"departure.iataCode" === "" || $"arrival.iataCode" === ""))
  val flightPrefixed = flightFiltered.toDF(flightFiltered.columns.map(x => s"FLIGHT_$x"): _*)

  val airport = spark.read.format("json").option("multiLine", true).load("src/main/resources/flightstream/airportDatabase.json")
  val departureAirportPrefixed = airport.toDF(airport.columns.map(x => s"DEPARTURE_$x"): _*)
  val arrivalAirportPrefixed = airport.toDF(airport.columns.map(x => s"ARRIVAL_$x"): _*)

  val airline = spark.read.format("json").option("multiLine", true).load("src/main/resources/flightstream/airlineDatabase.json")
  val airlineFiltered = airline.filter($"statusAirline" === "active")
  val airlinePrefixed = airlineFiltered.toDF(airlineFiltered.columns.map(x => s"AIRLINE_$x"): _*)

  val airplane = spark.read.format("json").option("multiLine", true).load("src/main/resources/flightstream/airplaneDatabase.json")
  val airplaneUpdated = airplane.withColumn("numberRegistration", regexp_replace($"numberRegistration", "-", ""))
  val airplanePrefixed = airplaneUpdated.toDF(airplaneUpdated.columns.map(x => s"AIRPLANE_$x"): _*)

  val bigFlights =
    flightPrefixed
      .join(departureAirportPrefixed, $"FLIGHT_departure.iatacode" === $"DEPARTURE_codeIataAirport")
      .join(arrivalAirportPrefixed, $"FLIGHT_arrival.iatacode" === $"ARRIVAL_codeIataAirport")
      .join(airlinePrefixed, $"FLIGHT_airline.icaocode" === $"AIRLINE_codeIcaoAirline")
      .join(airplanePrefixed, $"FLIGHT_aircraft.regNumber" === $"AIRPLANE_numberRegistration")

  val flightReceived = bigFlights.select(
    $"FLIGHT_flight.iataNumber".as("iataNumber"),
    $"FLIGHT_flight.icaoNumber".as("icaoNumber"),
    $"FLIGHT_geography".as("geography"),
    $"FLIGHT_speed.horizontal".as("speed"),
    struct(
      $"DEPARTURE_codeIcaoAirport",
      $"DEPARTURE_nameAirport",
      $"DEPARTURE_nameCountry",
      $"DEPARTURE_codeIso2Country",
      $"DEPARTURE_timezone",
      $"DEPARTURE_GMT",
    ).as("airportDeparture"),
    struct(
      $"ARRIVAL_codeIcaoAirport",
      $"ARRIVAL_nameAirport",
      $"ARRIVAL_nameCountry",
      $"ARRIVAL_codeIso2Country",
      $"ARRIVAL_timezone",
      $"ARRIVAL_GMT",
    ).as("airportArrival"),
    struct(
      $"AIRLINE_codeIcaoAirline",
      $"AIRLINE_nameAirline",
      $"AIRLINE_sizeAirline"
    ).as("airline"),
    struct(
      $"AIRPLANE_numberRegistration",
      $"AIRPLANE_productionLine",
      $"AIRPLANE_modelCode"
    ).as("airplane"),
    $"FLIGHT_status".as("status"),
    $"FLIGHT_system.updated".as("updated")
  )

  flightReceived.printSchema()

  spark.stop()

}
