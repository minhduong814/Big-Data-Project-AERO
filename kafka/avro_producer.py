import logging
import os
from uuid import uuid4

from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

import utils
from admin import Admin
from producer import ProducerClass
from schema_registry_client import SchemaClient
from confluent_kafka import KafkaException

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(
            f"Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )

class AvroProducer(ProducerClass):
    def __init__(
        self,
        bootstrap_server,
        topic,
        schema_registry_client,
        schema_str,
        compression_type=None,
        message_size=None,
        batch_size=None,
        waiting_time=None,
    ):
        super().__init__(
            bootstrap_server,
            topic,
            compression_type,
            message_size,
            batch_size,
            waiting_time,
        )
        self.schema_registry_client = schema_registry_client
        self.schema_str = schema_str
        self.avro_serializer = AvroSerializer(schema_registry_client, schema_str)
        self.string_serializer = StringSerializer("utf-8")

    def send_message(self, key=None, value=None):
        try:
            if value:
                byte_value = self.avro_serializer(
                    value, SerializationContext(topic, MessageField.VALUE)
                )
            else:
                byte_value = None
            self.producer.produce(
                topic=self.topic,
                key=self.string_serializer(str(key)),
                value=byte_value,
                headers={"correlation_id": str(uuid4())},
                on_delivery=delivery_report,
            )
            logging.info("Message Successfully Produce by the Producer")
        except KafkaException as e:
            kafka_error = e.args[0]
            if kafka_error.MSG_SIZE_TOO_LARGE:
                logging.error(
                    f"{e} , Current Message size is {len(value) / (1024 * 1024)} MB"
                )
        except Exception as e:
            logging.error(f"Error while sending message: {e}")

if __name__ == "__main__":
    # Get the directory where the current script is located
    path = os.path.dirname(os.path.abspath(__file__))

    # Load the Avro schema files for key and value
    value_schema_file = os.path.join(path,"flight-value.avsc")     # Update with your value schema file

    utils.configure_logging()
    schema_registry_url = "http://localhost:8081"
    bootstrap_servers = "localhost:9092"
    topic = "flights"
    schema_type = "AVRO"

    # Create Topic
    admin = Admin(bootstrap_servers)
    admin.create_topic(topic, 2)  # second parameter is for number of partitions

    with open(value_schema_file) as avro_schema_file:
        value_avro_schema = avro_schema_file.read()

    schema_client = SchemaClient(schema_registry_url, topic, value_avro_schema, schema_type)
    schema_client.set_compatibility("BACKWARD")
    schema_client.register_schema()

    # fetch schema_str from Schema Registry
    schema_str = schema_client.get_schema_str()
    # Produce messages
    producer = AvroProducer(
        bootstrap_servers,
        topic,
        schema_client.schema_registry_client,
        schema_str,
        compression_type="snappy",
        message_size=3 * 1024 * 1024,
        batch_size=10_00_00,  # in bytes, 1 MB
        waiting_time=10_000,  # in milliseconds, 10 seconds
    )
         
    # Read schema files into strings
    # Define Kafka producer configuration
    # Sample key and value
    key = {"flight_id": "12345"}  # Replace with your key structure
    value = {
        "flight_date": "2024-11-22",
        "flight_status": "landed",
        "departure": {
            "airport": "LHR",
            "timezone": "Europe/London",
            "iata": "LHR",
            "icao": "EGLL",
            "terminal": "5",
            "gate": "A10",
            "delay": None,
            "scheduled": "2024-11-22T14:30:00Z",
            "estimated": "2024-11-22T14:35:00Z",
            "actual": None,
            "estimated_runway": None,
            "actual_runway": None,
        },
        "arrival": {
            "airport": "JFK",
            "timezone": "America/New_York",
            "iata": "JFK",
            "icao": "KJFK",
            "terminal": "4",
            "gate": "B20",
            "baggage": "12",
            "delay": None,
            "scheduled": "2024-11-22T18:30:00Z",
            "estimated": "2024-11-22T18:35:00Z",
            "actual": None,
            "estimated_runway": None,
            "actual_runway": None,
        },
        "airline": {
            "name": "British Airways",
            "iata": "BA",
            "icao": "BAW",
        },
        "flight": {
            "number": "BA123",
            "iata": "BA123",
            "icao": "BAW123",
            "codeshared": {},
        },
    }

    try:
        producer.send_message(key=key, value=value)
    except Exception as e:
        print(f"Error message: {e}")
    producer.commit()

