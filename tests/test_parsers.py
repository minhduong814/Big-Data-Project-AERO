import pytest
from kafka.parsers import process_data_from_api


def test_process_data_from_api_basic():
    flight = {
        "flight_date": "2024-01-01",
        "flight": {"iata": "BA123"},
        "departure": {"iata": "LHR", "delay": 5},
        "arrival": {"iata": "JFK", "delay": None},
    }

    key, value = process_data_from_api(flight)

    assert key == {"flight_id": "2024-01-01_BA123_LHR_JFK"}
    assert value["departure"]["delay"] == "5"
    assert value["arrival"]["delay"] is None


def test_process_data_handles_missing_fields():
    flight = {}
    key, value = process_data_from_api(flight)
    assert "flight_id" in key
    assert isinstance(value, dict)
