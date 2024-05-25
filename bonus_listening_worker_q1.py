"""
This program listens for messages from the scientific_name_queue and processes them.
Transforms the Scientific_Name to uppercase and writes to a new CSV file.

Author: Derek Graves
Date: May 24, 2024
"""

import pika
import csv
import sys
import time
from util_logger import setup_logger

# Set up logger
logger, logname = setup_logger(__file__)

# Configuration variables
HOST = "localhost"
QUEUE_NAME = "scientific_name_queue"
OUTPUT_FILE = "processed_scientific_names.csv"

def callback(ch, method, properties, body):
    """Define behavior on getting a message."""
    scientific_name = body.decode()
    transformed_name = scientific_name.upper()
    logger.info(f"Processed Scientific Name: {transformed_name}")

    # Write the original and transformed data to a CSV file
    with open(OUTPUT_FILE, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([scientific_name, transformed_name])

    ch.basic_ack(delivery_tag=method.delivery_tag)

def main(host: str = HOST, queue_name: str = QUEUE_NAME):
    """Continuously listen for task messages on a named queue."""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host))
    except Exception as e:
        logger.error(f"Connection to RabbitMQ server failed. Verify the server is running on host={host}. The error says: {e}")
        sys.exit(1)

    try:
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queue_name, on_message_callback=callback)
        logger.info("Ready for work. To exit press CTRL+C")
        channel.start_consuming()
    except Exception as e:
        logger.error(f"Something went wrong. The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        logger.info("Closing connection. Goodbye.")
        connection.close()

if __name__ == "__main__":
    main()




