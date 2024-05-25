"""
This program reads data from a CSV file and sends specific columns to different queues on the RabbitMQ server.
Adds logging instead of print statements for better visibility.

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
QUEUE_1 = "scientific_name_queue"
QUEUE_2 = "common_name_queue"
QUEUE_3 = "target_h5_hpai_queue"
CSV_FILE = "avian-influenza-dwg.csv"
DELAY = 1  # Delay in seconds between sending tasks

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """
    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        ch.basic_publish(exchange="", routing_key=queue_name, body=message,
                         properties=pika.BasicProperties(delivery_mode=2))  # make message persistent
        logger.info(f"Sent {message} to {queue_name}")
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        if conn.is_open:
            conn.close()

def read_and_send_tasks(filename=CSV_FILE):
    """Read tasks from a CSV file and send them to appropriate queues"""
    try:
        with open(filename, newline='') as csvfile:
            task_reader = csv.DictReader(csvfile)
            for row in task_reader:
                send_message(HOST, QUEUE_1, row['Scientific_Name'])
                send_message(HOST, QUEUE_2, row['Common_Name'])
                send_message(HOST, QUEUE_3, row['target_H5_HPAI'])
                time.sleep(DELAY)  # Simulate delay for sending tasks
    except FileNotFoundError:
        logger.error(f"File {filename} not found.")
        sys.exit(1)

if __name__ == "__main__":
    read_and_send_tasks()



