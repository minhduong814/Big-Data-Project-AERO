import sys
import os
import json
import time
import requests
from kafka import KafkaProducer

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.settings import Config

def fetch_flight_data():
    """
    Fetch real-time flight data from Aviation Stack API.
    """
    params = {
        'access_key': Config.AVIATION_STACK_API_KEY,
        'limit': 100,
        'flight_status': 'active'
    }
    
    try:
        # Note: This is a placeholder URL. 
        # Aviation Stack API endpoint: http://api.aviationstack.com/v1/flights
        url = "http://api.aviationstack.com/v1/flights"
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return data.get('data', [])
    except Exception as e:
        print(f"Error fetching data from API: {e}")
        return []

def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print(f"Starting Kafka Producer to topic: {Config.KAFKA_TOPIC}")
    
    while True:
        flights = fetch_flight_data()
        if flights:
            print(f"Fetched {len(flights)} flights. Sending to Kafka...")
            for flight in flights:
                producer.send(Config.KAFKA_TOPIC, flight)
            producer.flush()
        else:
            print("No flights fetched or error occurred.")
        
        # Sleep for a while to avoid hitting API rate limits
        # Free plan is usually limited, so sleep 60 seconds or more
        time.sleep(60)

if __name__ == "__main__":
    run_producer()
