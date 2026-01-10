def process_data_from_api(flight: dict):
    """Normalize flight dict payload into key and cleaned value.
    This function is pure and safe to unit-test without heavy imports.
    """
    flight_date = flight.get("flight_date", "") or ""
    flight_iata = (flight.get("flight", {}) or {}).get("iata", "") or ""
    departure_iata = (flight.get("departure", {}) or {}).get("iata", "") or ""
    arrival_iata = (flight.get("arrival", {}) or {}).get("iata", "") or ""

    flight_id = f"{flight_date}_{flight_iata}_{departure_iata}_{arrival_iata}"
    key = {"flight_id": flight_id}

    # Ensure nested structures exist and normalize delays to strings
    if flight.get("flight", {}).get("codeshared") is None:
        flight.setdefault("flight", {})["codeshared"] = {}

    if "departure" in flight and "delay" in flight["departure"]:
        flight["departure"]["delay"] = (
            str(flight["departure"]["delay"]) if flight["departure"]["delay"] is not None else None
        )

    if "arrival" in flight and "delay" in flight["arrival"]:
        flight["arrival"]["delay"] = (
            str(flight["arrival"]["delay"]) if flight["arrival"]["delay"] is not None else None
        )

    return key, flight
