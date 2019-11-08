package flightstream.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.MethodDirectives.{get, post}
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import flightstream.http.Constants._
import flightstream.model.{FlightRequest, FlightStreamJsonProtocol}
import flightstream.spark.Aggregator._
import org.apache.spark.sql.DataFrame

trait FlightRoutes extends FlightStreamJsonProtocol with SprayJsonSupport {

  def routes(flightReceived: DataFrame) : Route =
    (pathPrefix(PREFIX_PATH) & pathEndOrSingleSlash) {
      get {
        complete(WELCOME_MESSAGE)
      } ~
        (post & entity(as[FlightRequest])) {
          case FlightRequest(TOTAL_FLIGHT_REQUEST, _) => complete(getTotalFlight(flightReceived))
          case FlightRequest(TOTAL_AIRLINE_REQUEST, _) => complete(getTotalAirline(flightReceived))
          case FlightRequest(TOP_DEPARTURE_REQUEST, limit) => complete(getTopDeparture(flightReceived, limit))
          case FlightRequest(TOP_ARRIVAL_REQUEST, limit) => complete(getTopArrival(flightReceived, limit))
          case FlightRequest(TOP_AIRLINE_REQUEST, limit) => complete(getTopAirline(flightReceived, limit))
          case FlightRequest(TOP_SPEED_REQUEST, limit) => complete(getTopSpeed(flightReceived, limit))
          case _ => complete(ERROR_RESPONSE)
        }
    }

}
