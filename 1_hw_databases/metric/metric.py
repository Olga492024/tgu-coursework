import pika
import json
import time
import pandas as pd
import os
import csv

os.makedirs('logs', exist_ok=True)

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
channel.queue_declare(queue='predictions', durable=True)
channel.queue_declare(queue='answers', durable=True)

# Инициализируем CSV, если его нет
csv_path = 'logs/metric_log.csv'
file_exists = os.path.exists(csv_path)
if not file_exists:
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'true_answer', 'prediction', 'absolute_error'])

# Кэши для синхронизации
predictions_cache = {}
answers_cache = {}
count = 0

def check_and_write(msg_id):
    """Проверяем, получены ли оба значения (prediction и answer) для ID"""
    if msg_id in predictions_cache and msg_id in answers_cache:
        prediction = predictions_cache[msg_id]
        true_answer = answers_cache[msg_id]
        absolute_error = abs(prediction - true_answer)

        # Используем запятую как разделитель (стандарт CSV)
        with open(csv_path, 'a', newline='') as f:
            writer = csv.writer(f, delimiter=',')
            writer.writerow([msg_id, true_answer, prediction, absolute_error])

        print(f"Recorded: id={msg_id}, true={true_answer}, pred={prediction:.4f}, error={absolute_error:.4f}")

        del predictions_cache[msg_id]
        del answers_cache[msg_id]
def predictions_callback(ch, method, properties, body):
    global count
    try:
        msg = json.loads(body)
        count += 1
        msg_id = msg['id']
        predictions_cache[msg_id] = msg['prediction']
        print(f"Got prediction #{count}: id={msg_id}, prediction={msg['prediction']:.4f}")
        check_and_write(msg_id)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error in predictions_callback: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def answers_callback(ch, method, properties, body):
    try:
        msg = json.loads(body)
        msg_id = msg['id']
        answers_cache[msg_id] = msg['true_answer']
        print(f"Got answer: id={msg_id}, true_answer={msg['true_answer']}")
        check_and_write(msg_id)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error in answers_callback: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


channel.basic_consume(queue='predictions', on_message_callback=predictions_callback)
channel.basic_consume(queue='answers', on_message_callback=answers_callback)

print("Metric service started...")
try:
    channel.start_consuming()
except KeyboardInterrupt:
    print("Shutting down...")
    channel.stop_consuming()
    connection.close()