package flightstream.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.MethodDirectives.{get, post}
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import flightstream.http.Constants._
import flightstream.model.{FlightRequest, FlightStreamJsonProtocol}
import flightstream.spark.Aggregation._

trait FlightRoutes extends FlightStreamJsonProtocol with SprayJsonSupport {

  lazy val routes : Route =
    (pathPrefix(ROOT_PATH) & pathEndOrSingleSlash) {
      get {
        complete(WELCOME_MESSAGE)
      } ~
        (post & entity(as[FlightRequest])) {
          case FlightRequest(TOTAL_FLIGHT_REQUEST, _) => complete(getTotalFlight)
          case FlightRequest(TOTAL_AIRLINE_REQUEST, _) => complete(getTotalAirline)
          case FlightRequest(TOP_DEPARTURE_REQUEST, limit) => complete(getTopDeparture(limit))
          case FlightRequest(TOP_ARRIVAL_REQUEST, limit) => complete(getTopArrival(limit))
          case FlightRequest(TOP_AIRLINE_REQUEST, limit) => complete(getTopAirline(limit))
          case FlightRequest(TOP_SPEED_REQUEST, limit) => complete(getTopSpeed(limit))
          case _ => complete(ERROR_RESPONSE)
        }
    }

}
