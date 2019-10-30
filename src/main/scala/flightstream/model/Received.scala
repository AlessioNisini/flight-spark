package flightstream.model

final case class FlightReceived(
  iataNumber: String,
  icaoNumber: String,
  geography: GeographyInfo,
  speed: Double,
  airportDeparture: AirportInfo,
  airportArrival: AirportInfo,
  airline: AirlineInfo,
  airplane: AirplaneInfo,
  status: String,
  updated: String
)

final case class AirplaneInfo(
  numberRegistration: String,
  productionLine: String,
  modelCode: String
)

final case class AirlineInfo(
  codeAirline: String,
  nameAirline: String,
  sizeAirline: String
)

final case class AirportInfo(
  codeAirport: String,
  nameAirport: String,
  nameCountry: String,
  codeIso2Country: String,
  timezone: String,
  gmt: String
)

final case class GeographyInfo(
  latitude: Double,
  longitude: Double,
  altitude: Double,
  direction: Double
)
