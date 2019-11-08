package flightstream

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import flightstream.http.Constants._
import flightstream.http.{FlightRoutes, SparkSessionWrapper}
import flightstream.model.{FlightRequest, FlightStreamJsonProtocol, OutputMessage, TopDeparture}
import flightstream.spark.{Aggregator, FlightReceivedBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Matchers, Outcome, WordSpec}


class EntryPointSpec
    extends WordSpec
    with SparkSessionWrapper
    with FlightStreamJsonProtocol
    with SprayJsonSupport
    with Matchers
    with ScalatestRouteTest
    with BeforeAndAfterAll {

  val aggregator: Aggregator = new Aggregator()
  val flightReceived: DataFrame = new FlightReceivedBuilder().build(
    s"$TEST_ROOT_PATH/flights.json",
    s"$TEST_ROOT_PATH/airportDatabase.json",
    s"$TEST_ROOT_PATH/airlineDatabase.json",
    s"$TEST_ROOT_PATH/airplaneDatabase.json"
  )
  val routes: Route = new FlightRoutes(aggregator, flightReceived).routes

  "FlightStream" should {

    "return the welcome msg for a GET request" in {
      Get("/flightstream") ~> routes ~> check {
        status shouldBe StatusCodes.OK
        entityAs[String] shouldBe WELCOME_MESSAGE
      }
    }

    "return a correct content for a POST request" in {
      Post("/flightstream", FlightRequest(TOP_DEPARTURE_REQUEST, 3)) ~> routes ~> check {
        status shouldBe StatusCodes.OK
        entityAs[List[OutputMessage]] shouldBe List(
          TopDeparture("A",3),
          TopDeparture("B",2),
          TopDeparture("C",2)
        )
      }
    }

  }

  override def afterAll(): Unit = {
    stopSpark()
    super.afterAll()
  }

}