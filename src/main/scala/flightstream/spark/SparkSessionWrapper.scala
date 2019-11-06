package flightstream.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{regexp_replace, struct}

trait SparkSessionWrapper {

  import CustomImplicits._

  lazy val spark: SparkSession = {
    val spark = SparkSession.builder.master("local[*]").appName("Flight Stream").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  lazy val flight: DataFrame = spark.readAndLoadJson("flights.json")
  lazy val airport: DataFrame = spark.readAndLoadJson("airportDatabase.json")
  lazy val airline: DataFrame = spark.readAndLoadJson("airlineDatabase.json")
  lazy val airplane: DataFrame = spark.readAndLoadJson("airplaneDatabase.json")

  lazy val flightReceived: DataFrame = buildFlightReceived(flight, airport, airline, airplane)

  def buildFlightReceived(
    flight: DataFrame,
    airport: DataFrame,
    airline: DataFrame,
    airplane: DataFrame
  ): DataFrame = {

    import spark.implicits._

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
