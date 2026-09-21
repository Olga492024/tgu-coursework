import pika
import json
import time
import random
import sys

sys.stdout = sys.stderr
max_retries = 10
for attempt in range(max_retries):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        print("Connected to RabbitMQ!")
        break
    except pika.exceptions.AMQPConnectionError:
        print(f"Attempt {attempt + 1}/{max_retries}: Waiting for RabbitMQ...")
        time.sleep(2)
else:
    raise Exception("Failed to connect to RabbitMQ")

channel = connection.channel()
channel.queue_declare(queue='features', durable=True)
channel.queue_declare(queue='answers', durable=True)

iteration = 0

while True:
    iteration += 1
    print(f"\n{'='*50}")
    print(f"ITERATION {iteration} STARTED")
    print(f"{'='*50}")

    # Отправляем features
    for i in range(5):
        msg_id = f"{iteration}_{i}"
        features = [float(j) for j in range(10)]
        msg = {'id': msg_id, 'features': features}

        channel.basic_publish(
            exchange='',
            routing_key='features',
            body=json.dumps(msg)
        )
        print(f"Sent features for id={msg_id}")
        time.sleep(1)

    random_index = random.randint(0, 4)
    random_id = f"{iteration}_{random_index}"
    delay_time = random.uniform(5, 15)
    print(f"\nRandom delay for id={random_id}: {delay_time:.2f} seconds")
    time.sleep(delay_time)

    print("\nSending answers...")
    for i in range(5):
        msg_id = f"{iteration}_{i}"
        true_answer = {'id': msg_id, 'true_answer': 2 * i + 1}

        channel.basic_publish(
            exchange='',
            routing_key='answers',
            body=json.dumps(true_answer)
        )
        print(f"Sent answer for id={msg_id}")
        time.sleep(1)

    print(f"\nIteration {iteration} completed. Waiting 10 seconds...")
    time.sleep(10)