# streaming-04-bonus-derek-graves

## Overview

This project demonstrates a RabbitMQ task processing setup with a producer and three consumers. The producer reads data from `avian-influenza-dwg.csv` and sends specific columns to different queues. The consumers process the data and write the original and transformed values to new CSV files.

## Author

Derek Graves  
Date: May 24, 2024

## Setup

### Virtual Environment

1. Create a virtual environment:
```bash
py -m venv .venv
```
2. Activate the environment:
```bash
source .venv/Scripts/activate
```
3.  Install required external packages:
```bash
pip install pika
```
or you may choose to install from a requirements.txt file:

```bash
pip install -r requirements.txt
```

### RabbitMQ Installation:
- Follow instructions on https://www.rabbitmq.com/tutorials to learn about installation.
- Ensure the RabbitMQ server is running on your maching once installed.
- Read the [RabbitMQ Tutorial - Work Queues](https://www.rabbitmq.com/tutorials/tutorial-two-python.html)

## Project Structure
- bonus_emitter.py: Reads data from avian-influenza-dwg.csv and sends specific columns to different queues.

The following code was added to ensure messages are sent by column to consumers:
```bash
def main():
    offer_rabbitmq_admin_site(SHOW_OFFER)

    tasks = read_tasks_from_csv(CSV_FILE)

    for task in tasks:
        columns = task.split(',')
        scientific_name = columns[1]
        common_name = columns[2]
        target_h5_hpai = columns[16]

        send_message_to_queue(HOST, QUEUE1, scientific_name)
        send_message_to_queue(HOST, QUEUE2, common_name)
        send_message_to_queue(HOST, QUEUE3, target_h5_hpai)
```
- bonus_consumer1.py: Processes Scientific_Name from the first queue and transforms it to uppercase, then writes to processed_scientific_names.csv
- bonus_consumer2.py: Processes Common_Name from the second queue and transforms it to uppercase, then writes to processed_common_names.csv.
- bonus_consumer3.py: Processes target_H5_HPAI from the third queue and formats it, then writes to processed_target_h5_hpai.csv.

Here is an example of how consumers were altered to only recieve messages for certain columns and transform them while writing to a new .csv file:
```bash
def callback(ch, method, properties, body):
    scientific_name = body.decode()
    transformed_name = scientific_name.upper()
    logger.info(f"Processed Scientific Name: {transformed_name}")

    with open(OUTPUT_FILE, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([scientific_name, transformed_name])

    ch.basic_ack(delivery_tag=method.delivery_tag)

def main(host=HOST, queue_name=QUEUE_NAME):
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
```
       
## Running the Project
1. Start RabbitMQ server.
2. Run emitter script:
```bash
python bonus_emitter.py
```
3. Open multiple terminals and run each consumer script in a separate terminal:
```bash
python bonus_consumer1.py
python bonus_consumer2.py
python bonus_consumer3.py
```
4. To stop either emitter or consumer scripts from streaming, use ctrl+c in the command prompt to stop the process.

5.  Review generated CSV files (there should be three) for accuracy - did each file only record their respective column of data and transform that data? 

## Screenshot of Project Terminals Executing

![Project Execution](images/BonusProject2024-05-24%20183049.png)

## References 

- Original project source: https://github.com/denisecase/streaming-04-multiple-consumers
- Dgraves 4 Project source: https://github.com/dgraves4/streaming-04-multiple-consumers
- Rabbit MQ Tutorials: https://www.rabbitmq.com/tutorials 


