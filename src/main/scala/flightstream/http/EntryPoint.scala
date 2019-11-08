package flightstream.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import flightstream.http.Constants._
import flightstream.spark.FlightReceivedBuilder._
import flightstream.spark.SparkSessionWrapper

object EntryPoint extends App with FlightRoutes with SparkSessionWrapper {

  implicit val system: ActorSystem = ActorSystem("FlightStream")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val rawData = loadData(
    s"$FILE_ROOT_PATH/flights.json",
    s"$FILE_ROOT_PATH/airportDatabase.json",
    s"$FILE_ROOT_PATH/airlineDatabase.json",
    s"$FILE_ROOT_PATH/airplaneDatabase.json"
  )

  val rawPrefixedData = transformData(rawData)

  val flightReceived = buildFlightReceived(rawPrefixedData)

  Http().bindAndHandle(routes(flightReceived), HOST_NAME, PORT_NUMBER)

}