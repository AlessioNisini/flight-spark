package flightstream.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import flightstream.http.Constants._
import flightstream.spark.{Aggregator, FlightReceivedBuilder}

object EntryPoint extends App with SparkSessionWrapper {

  implicit val system: ActorSystem = ActorSystem("FlightStream")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val aggregator = new Aggregator()
  val flightReceived = new FlightReceivedBuilder().build(
    s"$MAIN_ROOT_PATH/flights.json",
    s"$MAIN_ROOT_PATH/airportDatabase.json",
    s"$MAIN_ROOT_PATH/airlineDatabase.json",
    s"$MAIN_ROOT_PATH/airplaneDatabase.json"
  )
  val routes = new FlightRoutes(aggregator, flightReceived).routes

  Http().bindAndHandle(routes, HOST_NAME, PORT_NUMBER)

}