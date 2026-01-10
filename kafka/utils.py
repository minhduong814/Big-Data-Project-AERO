import os

from dotenv import load_dotenv

import logging


def configure_logging():
    """Configure a consistent logging format for the application."""
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    fmt = os.getenv(
        "LOG_FORMAT",
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    logging.basicConfig(level=level, format=fmt)


def load_env():
    """Loads environment variables from a .env file"""

    dotenv_path = os.path.join(os.path.dirname(__file__), "../.env")
    load_dotenv(dotenv_path)


def delivery_report(err, msg):
    logger = logging.getLogger("kafka.delivery")
    if err is not None:
        logger.error("Delivery failed for record %s: %s", msg.key(), err)
    else:
        logger.info(
            "Record %s successfully produced to %s [%s] at offset %s",
            msg.key(),
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )