"""
This program listens for work messages continuously.
Start multiple versions to add more workers.
Logs the received tasks and their completion for better visibility.

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
QUEUE_NAME = "dgraves4_task_queue"

# Define a callback function to be called when a message is received
def callback(ch, method, properties, body):
    """Define behavior on getting a message."""
    # Decode the binary message body to a string
    logger.info(f"Received {body.decode()}")
    # Simulate work by sleeping for the number of dots in the message
    time.sleep(body.count(b"."))
    # When done with task, tell the user
    logger.info("Done")
    # Acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Define a main function to run the program
def main(host: str = HOST, queue_name: str = QUEUE_NAME):
    """Continuously listen for task messages on a named queue."""
    # When a statement can go wrong, use a try-except block
    try:
        # Try this code, if it works, keep going
        # Create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host))

    # Except, if there's an error, do this
    except Exception as e:
        logger.error(f"Connection to RabbitMQ server failed. Verify the server is running on host={host}. The error says: {e}")
        sys.exit(1)

    try:
        # Use the connection to create a communication channel
        channel = connection.channel()

        # Use the channel to declare a durable queue
        channel.queue_declare(queue=queue_name, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        channel.basic_qos(prefetch_count=1)

        # Configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume(queue=queue_name, on_message_callback=callback)

        # Print a message to the console for the user
        logger.info("Ready for work. To exit press CTRL+C")

        # Start consuming messages via the communication channel
        channel.start_consuming()

    # Except, in the event of an error OR user stops the process, do this
    except Exception as e:
        logger.error(f"Something went wrong. The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        logger.info("Closing connection. Goodbye.")
        connection.close()

# Standard Python idiom to indicate main program entry point
if __name__ == "__main__":
    # Call the main function with the information needed
    main()


