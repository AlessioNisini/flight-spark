package flightstream.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import flightstream.http.Constants._

object EntryPoint extends App with FlightRoutes {

  implicit val system: ActorSystem = ActorSystem("FlightStream")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  Http().bindAndHandle(routes, HOST_NAME, PORT_NUMBER)

}