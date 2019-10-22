package flightstream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FlightStream extends App {

  val spark = SparkSession.builder.master("local[*]").appName("Simple Application").getOrCreate()

  spark.conf.set("spark.sql.shuffle.partitions", "5")
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._
  import CustomImplicits._

  val beforeLoading = java.lang.System.currentTimeMillis()

  val flight = spark.readAndLoadJson("flights.json")
  val airport = spark.readAndLoadJson("airportDatabase.json")
  val airline = spark.readAndLoadJson("airlineDatabase.json")
  val airplane = spark.readAndLoadJson("airplaneDatabase.json")

  val beforePlanning = java.lang.System.currentTimeMillis()

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

  val beforeExecution = java.lang.System.currentTimeMillis()

  val totalFlight = flightReceived.count()

  //  val totalAirline = flightReceived.as[FlightReceived].map(_.airline.codeAirline).distinct().count()   // 3141
  //  val totalAirline = flightReceived.groupBy($"airline.codeAirline").count().count()  //2000
  val totalAirline = flightReceived.select(countDistinct($"airline.codeAirline")).first().getLong(0)  //2362

  val topDeparture =
    flightReceived
      .groupBy($"airportDeparture.codeAirport")
      .count()
      .sort($"count".desc)
      .limit(5)
      .collect()
      .toList

  val topArrival =
    flightReceived
      .groupBy($"airportArrival.codeAirport")
      .count()
      .sort($"count".desc)
      .limit(5)
      .collect()
      .toList

  val topAirline =
    flightReceived
      .groupBy($"airline.nameAirline")
      .count()
      .sort($"count".desc)
      .limit(5)
      .collect()
      .toList

  val topSpeed =
    flightReceived
      .select($"icaoNumber", $"speed")
      .sort($"speed".desc)
      .limit(5)
      .collect()
      .toList

  val afterExecution = java.lang.System.currentTimeMillis()

  println(s"Loading Time: ${beforePlanning - beforeLoading}")
  println(s"Planning Time: ${beforeExecution - beforePlanning}")
  println(s"Execution Time: ${afterExecution - beforeExecution}")

  spark.stop()

}
