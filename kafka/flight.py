from typing import Optional, Dict
from decimal import Decimal
from datetime import datetime


class Flight:
    def __init__(self, flight_date: Optional[str], flight_status: Optional[str], departure: Dict, arrival: Dict, airline: Dict, flight: Dict):
        self.flight_date = flight_date
        self.flight_status = flight_status
        self.departure = Departure(**departure) if departure else None
        self.arrival = Arrival(**arrival) if arrival else None
        self.airline = Airline(**airline) if airline else None
        self.flight = FlightInfo(**flight) if flight else None

    @classmethod
    def from_dict(cls, d: Dict):
        return cls(
            flight_date=d.get("flight_date"),
            flight_status=d.get("flight_status"),
            departure=d.get("departure", {}),
            arrival=d.get("arrival", {}),
            airline=d.get("airline", {}),
            flight=d.get("flight", {})
        )

    def __repr__(self):
        return f"{self.__class__.__name__}: {self.__dict__}"


class Departure:
    def __init__(self, airport: Optional[str], timezone: Optional[str], iata: Optional[str], icao: Optional[str], 
                 terminal: Optional[str], gate: Optional[str], delay: Optional[str], scheduled: Optional[str], 
                 estimated: Optional[str], actual: Optional[str], estimated_runway: Optional[str], actual_runway: Optional[str]):
        self.airport = airport
        self.timezone = timezone
        self.iata = iata
        self.icao = icao
        self.terminal = terminal
        self.gate = gate
        self.delay = delay
        self.scheduled = scheduled
        self.estimated = estimated
        self.actual = actual
        self.estimated_runway = estimated_runway
        self.actual_runway = actual_runway


class Arrival:
    def __init__(self, airport: Optional[str], timezone: Optional[str], iata: Optional[str], icao: Optional[str], 
                 terminal: Optional[str], gate: Optional[str], baggage: Optional[str], delay: Optional[str], 
                 scheduled: Optional[str], estimated: Optional[str], actual: Optional[str], 
                 estimated_runway: Optional[str], actual_runway: Optional[str]):
        self.airport = airport
        self.timezone = timezone
        self.iata = iata
        self.icao = icao
        self.terminal = terminal
        self.gate = gate
        self.baggage = baggage
        self.delay = delay
        self.scheduled = scheduled
        self.estimated = estimated
        self.actual = actual
        self.estimated_runway = estimated_runway
        self.actual_runway = actual_runway


class Airline:
    def __init__(self, name: Optional[str], iata: Optional[str], icao: Optional[str]):
        self.name = name
        self.iata = iata
        self.icao = icao


class FlightInfo:
    def __init__(self, number: Optional[str], iata: Optional[str], icao: Optional[str], codeshared: Optional[Dict]):
        self.number = number
        self.iata = iata
        self.icao = icao
        self.codeshared = Codeshared(**codeshared) if codeshared else None


class Codeshared:
    def __init__(self, airline_name: Optional[str], airline_iata: Optional[str], airline_icao: Optional[str], 
                 flight_number: Optional[str], flight_iata: Optional[str], flight_icao: Optional[str]):
        self.airline_name = airline_name
        self.airline_iata = airline_iata
        self.airline_icao = airline_icao
        self.flight_number = flight_number
        self.flight_iata = flight_iata
        self.flight_icao = flight_icao
