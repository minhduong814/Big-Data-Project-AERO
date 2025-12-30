import json
import time

import requests
import random
from kafka import KafkaProducer


def main():
    API_KEY = "436ab0c1e41f8eb5624d87f45ac6946d"
    BASE_URL = "http://api.aviationstack.com/v1/flights"

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    airline_iata_list = [
        "AA", "DL", "UA", "WN", "B6", "AS", "NK", "F9", "HA", "G4",
        "VN", "VJ",
        "LH", "BA", "AF", "KL", "IB", "AY", "SK", "LX", "OS", "TP", "EI", "A3", "LO", "TK"
    ]

    while True:
        try:
            # Define parameters
            params = {
                "access_key": API_KEY,
                "airline_iata": random.choice(airline_iata_list),
                "dep_country": "United States",  # Filter by country
            }

            # Make the request
            response = requests.get(BASE_URL, params=params)

            # Parse and print the response
            if response.status_code == 200:
                data = response.json()

                if "data" in data:
                    for flight in data["data"]:
                        # Send each flight object as a separate message
                        producer.send("aviation.flights", value=flight)

                print(f"Sent {len(data.get('data', []))} flights to Kafka.")
            else:
                print(f"Error: {response.status_code}, {response.text}")

            # Respect API limits (Aviationstack free tier is limited)
            time.sleep(60)

        except Exception as e:
            print(f"Error: {e}")
            time.sleep(10)


if __name__ == "__main__":
    main()
