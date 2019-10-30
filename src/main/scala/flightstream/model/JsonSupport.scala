package flightstream.model

import spray.json._

final case class FlightRequest(source: String, limit: Int)

sealed trait Output extends Product
case class TotalFlight(count: BigInt) extends Output
case class TotalAirline(count: BigInt) extends Output
case class TopDeparture(code: String, count: BigInt) extends Output
case class TopArrival(code: String, count: BigInt) extends Output
case class TopAirline(name: String, count: BigInt) extends Output
case class TopSpeed(code: String, speed: Double) extends Output

trait FlightStreamJsonProtocol extends DefaultJsonProtocol {
  implicit val flightRequest: RootJsonFormat[FlightRequest] = jsonFormat2(FlightRequest)
  implicit val totalFlight: RootJsonFormat[TotalFlight] = jsonFormat1(TotalFlight)
  implicit val totalAirline: RootJsonFormat[TotalAirline] = jsonFormat1(TotalAirline)
  implicit val topDeparture: RootJsonFormat[TopDeparture] = jsonFormat2(TopDeparture)
  implicit val topArrival: RootJsonFormat[TopArrival] = jsonFormat2(TopArrival)
  implicit val topsAirline: RootJsonFormat[TopAirline] = jsonFormat2(TopAirline)
  implicit val topsSpeed: RootJsonFormat[TopSpeed] = jsonFormat2(TopSpeed)
  implicit val output: RootJsonFormat[Output] = new RootJsonFormat[Output] {
    def write(obj: Output): JsValue =
      JsObject((obj match {
        case x: TotalFlight => x.toJson
        case x: TotalAirline => x.toJson
        case x: TopDeparture => x.toJson
        case x: TopArrival => x.toJson
        case x: TopAirline => x.toJson
        case x: TopSpeed => x.toJson
      }).asJsObject.fields + ("type" -> JsString(obj.productPrefix)))
    def read(json: JsValue): Output = {
      val result = JsObject(json.asJsObject.fields - "type")
      json.asJsObject.getFields("type") match {
        case Seq(JsString("TotalFlight")) => result.convertTo[TotalFlight]
        case Seq(JsString("TotalAirline")) => result.convertTo[TotalAirline]
        case Seq(JsString("TopDeparture")) => result.convertTo[TopDeparture]
        case Seq(JsString("TopArrival")) => result.convertTo[TopArrival]
        case Seq(JsString("TopAirline")) => result.convertTo[TopAirline]
        case Seq(JsString("TopSpeed")) => result.convertTo[TopSpeed]
      }
    }
  }
}