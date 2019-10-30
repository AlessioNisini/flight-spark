package flightstream.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import flightstream.model._
import flightstream.spark.Aggregation._
import Constants._

object EntryPoint extends App with FlightStreamJsonProtocol with SprayJsonSupport {

  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()

  val route =
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

  Http().bindAndHandle(route, HOST_NAME, PORT_NUMBER)

}