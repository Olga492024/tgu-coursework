import pika
import json
import time

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
channel.queue_declare(queue='predictions', durable=True)

count = 0


def callback(ch, method, properties, body):
    global count
    try:
        msg = json.loads(body)
        count += 1

        prediction = sum(msg['features']) / len(msg['features'])

        prediction_msg = {
            'id': msg['id'],
            'prediction': prediction
        }

        ch.basic_publish(
            exchange='',
            routing_key='predictions',
            body=json.dumps(prediction_msg)
        )

        print(f"Processed #{count}: id={msg['id']} -> prediction={prediction:.4f}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


channel.basic_consume(queue='features', on_message_callback=callback)

print("Model service started, waiting for features...")
try:
    channel.start_consuming()
except KeyboardInterrupt:
    print("Shutting down...")
    channel.stop_consuming()
    connection.close()