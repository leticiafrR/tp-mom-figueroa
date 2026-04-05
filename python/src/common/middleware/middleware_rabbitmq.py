from math import e

import pika
from pika.exceptions import AMQPConnectionError
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareMessageError, MessageMiddlewareDisconnectedError, MessageMiddlewareCloseError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name)
        self.queue_name = queue_name

    def send(self, message):
        try:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        except AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ")

    def start_consuming(self, on_message_callback):

        def callback(ch, method, properties, body):
            try:
                on_message_callback(body, lambda: ch.basic_ack(delivery_tag=method.delivery_tag), lambda: ch.basic_nack(delivery_tag=method.delivery_tag))
            except MessageMiddlewareDisconnectedError:
                raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ")
            except Exception as e:
                raise MessageMiddlewareMessageError(f"An error occurred while processing the message: {str(e)}")
            
        self.channel.basic_consume(queue=self.queue_name,on_message_callback=callback, auto_ack=False)
        self.channel.start_consuming()
    
    def stop_consuming(self):

        self.channel.stop_consuming()

    def close(self):
        try:
            self.connection.close()
        except Exception:
            raise MessageMiddlewareCloseError("Failed to close the connection to RabbitMQ")
    

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass

    def start_consuming(self, on_message_callback):
        pass
    def stop_consuming(self):
        pass
    def send(self, message):
        pass
    def close(self):
        pass

