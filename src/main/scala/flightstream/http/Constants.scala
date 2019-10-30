package flightstream.http

import akka.http.scaladsl.model.{HttpEntity, HttpResponse, StatusCodes}

object Constants {

  final val WELCOME_MESSAGE =
    """
      |Welcome...
      |Do a POST with two fields:
      |- requestType (one of totalFlight, totalAirline, topDeparture, topArrival, topAirline, topSpeed)
      |- limit (a number)
      |""".stripMargin

  val ERROR_RESPONSE = HttpResponse(
    status = StatusCodes.BadRequest,
    entity = HttpEntity("Error: Unhandled request")
  )

  final val HOST_NAME = "localhost"
  final val PORT_NUMBER = 8080
  final val ROOT_PATH = "flightstream"

  final val TOTAL_FLIGHT_REQUEST = "totalFlight"
  final val TOTAL_AIRLINE_REQUEST = "totalAirline"
  final val TOP_DEPARTURE_REQUEST = "topDeparture"
  final val TOP_ARRIVAL_REQUEST = "topArrival"
  final val TOP_AIRLINE_REQUEST = "topAirline"
  final val TOP_SPEED_REQUEST = "topSpeed"

}
