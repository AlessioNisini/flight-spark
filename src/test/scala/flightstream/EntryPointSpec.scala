package flightstream

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import flightstream.http.Constants._
import flightstream.http.FlightRoutes
import flightstream.model.{FlightRequest, OutputMessage}
import flightstream.spark.SparkSessionWrapper
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

class EntryPointSpec
    extends WordSpec
    with FlightRoutes
    with SparkSessionWrapper
    with Matchers
    with ScalatestRouteTest
    with BeforeAndAfterAll {

  "FlightStream" should {

    "return the welcome msg for a GET request" in {
      Get("/flightstream") ~> routes ~> check {
        status shouldBe StatusCodes.OK
        entityAs[String] shouldBe WELCOME_MESSAGE
      }
    }

    "return a correct content for a POST request" in {
      Post("/flightstream", FlightRequest(TOP_DEPARTURE_REQUEST, 6)) ~> routes ~> check {
        status shouldBe StatusCodes.OK
        entityAs[List[OutputMessage]].size shouldBe 6
      }
    }

  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

}
