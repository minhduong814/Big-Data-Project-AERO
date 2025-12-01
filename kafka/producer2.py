import os

from utils import *
#from settings import KAFKA_ADDRESS
from json_producer import setting_up, process_data_from_api, request_data
import time
import pandas as pd

KAFKA_ADDRESS = "35.240.239.52:9092,35.240.239.52:9093"
if __name__=="__main__":
    producer = setting_up(bootstrap_servers=KAFKA_ADDRESS, topic="flights")
    access_key = os.environ.get("ACCESS_KEY", "0f1f4a0ab47894952b1e301b3f928910")

    url = "http://api.aviationstack.com/v1/flights"

    dirpath = os.path.dirname(os.path.abspath(__file__))
    # Read the CSV file into a DataFrame
    df = pd.read_csv(os.path.join(dirpath,'route_frequency.csv'))

    while True:
        try:
                
            # Sample one row based on the 'count' column as weights
            sampled_row = df.sample(n=1, weights='count', random_state=None).iloc[0]

            dep_iata = sampled_row['Origin']
            arr_iata = sampled_row['Dest']
            count = sampled_row['count']
            
            params = {
                'dep_iata': dep_iata,
                'arr_iata': arr_iata,
                'limit': 100
            }

            # Request data using the request_data function
            data = request_data(access_key, retries=3, delay=5, **params)

            # If data was returned, process it (e.g., print or save to a file)
            if data:
                print(f"Flight data for {dep_iata} -> {arr_iata}: {data}")
                for flight in data['data']:
                    key, value = process_data_from_api(flight)
                    try:
                        producer.send_message(key=key, value=value)
                    except Exception as e:
                        print(f"Error message: {e}")
                    producer.commit()
            else:
                print(f"No data found for {dep_iata} -> {arr_iata}")

        except Exception as e:
            print(f"An error occur during the execution: {e}")
            
        time.sleep(1000)