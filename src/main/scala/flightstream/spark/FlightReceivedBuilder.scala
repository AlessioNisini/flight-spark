package flightstream.spark

import flightstream.model.{RawData, RawPrefixedData}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{regexp_replace, struct}

object FlightReceivedBuilder extends SparkSessionWrapper {

  import spark.implicits._
  import CustomImplicits._

  def loadData(
    flightPath: String,
    airportPath: String,
    airlinePath: String,
    airplanePath: String
  ) : RawData = {
    RawData(
      spark.readAndLoadJson(flightPath),
      spark.readAndLoadJson(airportPath),
      spark.readAndLoadJson(airlinePath),
      spark.readAndLoadJson(airplanePath)
    )
  }

  def transformData(rawData: RawData): RawPrefixedData = {

    val flightFiltered = rawData.flight.filter($"status" === "en-route" && !($"departure.iataCode" === "" || $"arrival.iataCode" === ""))
    val flightPrefixed = flightFiltered.addPrefix("FLIGHT")

    val departureAirportPrefixed = rawData.airport.addPrefix("DEPARTURE")

    val arrivalAirportPrefixed = rawData.airport.addPrefix("ARRIVAL")

    val airlineFiltered = rawData.airline.filter($"statusAirline" === "active")
    val airlinePrefixed = airlineFiltered.addPrefix("AIRLINE")

    val airplaneUpdated = rawData.airplane.withColumn("numberRegistration", regexp_replace($"numberRegistration", "-", ""))
    val airplanePrefixed = airplaneUpdated.addPrefix("AIRPLANE")

    RawPrefixedData(flightPrefixed, departureAirportPrefixed, arrivalAirportPrefixed, airlinePrefixed, airplanePrefixed)

  }

  def buildFlightReceived(rawPrefixedData: RawPrefixedData): DataFrame = {

    val bigFlights =
      rawPrefixedData.flight
        .join(rawPrefixedData.departure, $"FLIGHT_departure.iatacode" === $"DEPARTURE_codeIataAirport")
        .join(rawPrefixedData.arrival, $"FLIGHT_arrival.iatacode" === $"ARRIVAL_codeIataAirport")
        .join(rawPrefixedData.airline, $"FLIGHT_airline.icaocode" === $"AIRLINE_codeIcaoAirline")
        .join(rawPrefixedData.airplane, $"FLIGHT_aircraft.regNumber" === $"AIRPLANE_numberRegistration")

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
