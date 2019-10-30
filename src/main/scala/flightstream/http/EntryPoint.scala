package flightstream.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import flightstream.model._
import flightstream.spark.FlightStream._

object EntryPoint extends App with FlightStreamJsonProtocol with SprayJsonSupport {

  implicit val system: ActorSystem = ActorSystem("FlightStream")
  implicit val mat: ActorMaterializer = ActorMaterializer()

  val route =
    (pathPrefix("flightstream") & pathEndOrSingleSlash) {
      get {
        complete("Welcome")
      } ~
        (post & entity(as[FlightRequest])) {
          case FlightRequest(source, _) if source == "totalFlight" => complete(getTotalFlight)
          case FlightRequest(source, _) if source == "totalAirline" => complete(getTotalAirline)
          case FlightRequest(source, limit) if source == "topDeparture" => complete(getTopDeparture(limit))
          case FlightRequest(source, limit) if source == "topArrival" => complete(getTopArrival(limit))
          case FlightRequest(source, limit) if source == "topAirline" => complete(getTopAirline(limit))
          case FlightRequest(source, limit) if source == "topSpeed" => complete(getTopSpeed(limit))
        }
    }

  Http().bindAndHandle(route, "localhost", 8080)

}