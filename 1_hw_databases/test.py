import pika
import json
import time

connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq', 5672))
channel = connection.channel()
channel.queue_declare(queue='features', durable=True)

test_msg = {
    'id': 999,
    'features': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
}

channel.basic_publish(
    exchange='',
    routing_key='features',
    body=json.dumps(test_msg)
)

print("Test message sent!")
connection.close()