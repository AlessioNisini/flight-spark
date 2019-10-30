package flightstream.model

final case class FlightRaw(
    geography: Geography,
    speed: Speed,
    departure: CommonCode,
    arrival: CommonCode,
    aircraft: Aircraft,
    airline: CommonCode,
    flight: Flight,
    system: System,
    status: String
)

final case class AirplaneRaw(
    numberRegistration: String,
    productionLine: String,
    airplaneIataType: String,
    planeModel: String,
    modelCode: String,
    hexIcaoAirplane: String,
    codeIataPlaneLong: String,
    planeOwner: String,
    enginesType: String,
    planeAge: String,
    planeStatus: String
)

final case class AirportRaw(
    airportId: String,
    nameAirport: String,
    codeIataAirport: String,
    latitudeAirport: String,
    longitudeAirport: String,
    nameCountry: String,
    codeIso2Country: String,
    codeIataCity: String,
    timezone: String,
    GMT: String
)

final case class AirlineRaw(
    airlineId: String,
    nameAirline: String,
    codeIataAirline: String,
    codeIcaoAirline: String,
    callsign: String,
    statusAirline: String,
    sizeAirline: String,
    nameCountry: String,
    codeIso2Country: String
)

final case class CityRaw(
    cityId: String,
    nameCity: String,
    codeIataCity: String,
    codeIso2Country: String,
    latitudeCity: String,
    longitudeCity: String
)

final case class Geography(
    latitude: Double,
    longitude: Double,
    altitude: Double,
    direction: Double
)

final case class Speed(
    horizontal: Double,
    vertical: Double
)

final case class CommonCode(
    iataCode: String,
    icaoCode: String
)

final case class Aircraft(
    regNumber: String,
    icaoCode: String,
    icao24: String,
    iataCode: String
)

final case class Flight(
    iataNumber: String,
    icaoNumber: String,
    number: String
)

final case class System(
    updated: String,
    squawk: String
)