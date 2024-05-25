"""
This program listens for messages from the target_h5_hpai_queue and processes them.
Transforms the target_H5_HPAI to a readable format.

Author: Derek Graves
Date: May 24, 2024
"""

import pika
import sys
import time
from util_logger import setup_logger

# Set up logger
logger, logname = setup_logger(__file__)

# Configuration variables
HOST = "localhost"
QUEUE_NAME = "target_h5_hpai_queue"

def callback(ch, method, properties, body):
    """Define behavior on getting a message."""
    target_h5_hpai = body.decode()
    readable_format = f"Target H5 HPAI: {target_h5_hpai}"
    logger.info(f"Processed Target H5 HPAI: {readable_format}")
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



