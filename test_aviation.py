import requests
import json
import sys


API_KEY = "436ab0c1e41f8eb5624d87f45ac6946d"
BASE_URL = "http://api.aviationstack.com/v1/flights"

# Define parameters
params = {
    "access_key": API_KEY,
    # "dep_iata": "HAN",
    #"dep_iata": "LHR",  # Filter flights departing from London Heathrow
    # OR
    # "arr_iata": "LGW",  # Filter flights arriving at London Gatwick
    # "dep_country": "United States",  # Filter by country
    "airline_iata": "VN",  # Vietnam Airlines
    "limit": 100000
}

# Make the request
response = requests.get(BASE_URL, params=params)

# Parse and print the response
if response.status_code == 200:
    data = response.json()

    with open('data/test.json', 'w') as file:
        json.dump(data, file)
    print(data)
else:
    print(f"Error: {response.status_code}, {response.text}")
