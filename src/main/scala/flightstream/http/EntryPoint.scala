package flightstream.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import flightstream.model._
import flightstream.spark.FlightStream._
import org.apache.spark.sql.SparkSession

object EntryPoint extends App with FlightStreamJsonProtocol with SprayJsonSupport {

  implicit val system: ActorSystem = ActorSystem("FlightStream")
  implicit val mat: ActorMaterializer = ActorMaterializer()

  implicit val spark: SparkSession = init()

  val route =
    (pathPrefix("flightstream") & pathEndOrSingleSlash) {
      get {
        complete("Welcome")
      } ~
        (post & entity(as[FlightRequest])) { req =>
          complete(run(req))
        }
    }

  Http().bindAndHandle(route, "localhost", 8080)

}