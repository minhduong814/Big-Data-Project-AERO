#DONT TOUCH THIS SCRIPT
import logging
import os

from confluent_kafka import Consumer

import utils


class ConsumerClass:
    def __init__(self, bootstrap_server, topic, group_id):
        """Initializes the consumer."""
        self.bootstrap_server = bootstrap_server
        self.topic = topic
        self.group_id = group_id
        self.consumer = Consumer(
            {"bootstrap.servers": bootstrap_server, "group.id": self.group_id}
        )

    def consume_messages(self):
        """Consume Messages from Kafka."""
        self.consumer.subscribe([self.topic])
        logging.info(f"Successfully subscribed to topic: {self.topic}")

        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logging.error(f"Consumer error: {msg.error()}")
                    continue
                byte_message = msg.value()
                decoded_message = byte_message.decode("utf-8")
                logging.info(
                    f"Byte message: {byte_message}, Type: {type(byte_message)}"
                )
                logging.info(
                    f"Decoded message: {decoded_message}, Type: {type(decoded_message)}"  # noqa: E501
                )
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()


if __name__ == "__main__":
    utils.load_env()
    utils.configure_logging()

    bootstrap_server = "35.240.239.52:9092"#os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    topic = "flights"#os.environ.get("KAFKA_TOPIC")
    group_id = 0
    consumer = ConsumerClass(bootstrap_server, topic, group_id)
    consumer.consume_messages()