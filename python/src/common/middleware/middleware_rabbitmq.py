import pika
from pika.exceptions import AMQPConnectionError
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareMessageError, MessageMiddlewareDisconnectedError, MessageMiddlewareCloseError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    """
    Cliente de cola RabbitMQ.

    Qué hace:
    - Permite enviar mensajes a una cola por nombre.
    - Permite consumir mensajes de esa misma cola.

    Qué proveer:
    - `host`: host/IP de RabbitMQ.
    - `queue_name`: nombre de la cola a usar.
    """

    def __init__(self, host, queue_name):
        """
        Crea una instancia asociada a una cola.

        Parámetros:
        - `host`: host/IP de RabbitMQ.
        - `queue_name`: nombre de la cola.
        """
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name)
        self.queue_name = queue_name

    def send(self, message):
        """
        Envía un mensaje a la cola configurada.

        Parámetros:
        - `message`: contenido a publicar (bytes).
        """
        try:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        except AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ")

    def start_consuming(self, on_message_callback):
        """
        Inicia el consumo bloqueante de mensajes.

        Parámetros:
        - `on_message_callback`: función con firma
          `callback(message, ack, nack)`.
        """

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
        """Detiene el consumo iniciado con `start_consuming()`."""
        self.channel.stop_consuming()

    def close(self):
        """Cierra la conexión de la instancia."""
        try:
            self.connection.close()
        except Exception:
            raise MessageMiddlewareCloseError("Failed to close the connection to RabbitMQ")
    

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    """
    Cliente de exchange RabbitMQ.

    Qué hace:
    - Permite consumir mensajes para una o más routing keys.
    - Permite publicar mensajes al exchange.

    Qué proveer:
    - `host`: host/IP de RabbitMQ.
    - `exchange_name`: nombre del exchange.
    - `routing_keys`: lista de routing keys a usar.
    """
    
    def __init__(self, host, exchange_name, routing_keys):
        """
        Crea una instancia asociada a un exchange y una lista de routing keys.

        Parámetros:
        - `host`: host/IP de RabbitMQ.
        - `exchange_name`: nombre del exchange.
        - `routing_keys`: routing keys de suscripción/publicación.
        """
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

        result = self.channel.queue_declare(queue='', exclusive=True, auto_delete=True)
        self.queue_name = result.method.queue

        for routing_key in routing_keys:
            self.channel.queue_bind(
                exchange=self.exchange_name,
                queue=self.queue_name,
                routing_key=routing_key,
            )

    def start_consuming(self, on_message_callback):
        """
        Inicia el consumo bloqueante de mensajes del exchange.

        Parámetros:
        - `on_message_callback`: función con firma
          `callback(message, ack, nack)`.
        """
        def callback(ch, method, properties, body):
            try:
                on_message_callback(
                    body,
                    lambda: ch.basic_ack(delivery_tag=method.delivery_tag),
                    lambda: ch.basic_nack(delivery_tag=method.delivery_tag),
                )
            except MessageMiddlewareDisconnectedError:
                raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ")
            except Exception as e:
                raise MessageMiddlewareMessageError(f"An error occurred while processing the message: {str(e)}")

        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=False)
        self.channel.start_consuming()

    def stop_consuming(self):
        """Detiene el loop de consumo iniciado con `start_consuming()`."""
        self.channel.stop_consuming()

    def send(self, message):
        """
        Publica el mensaje en el exchange.

        Parámetros:
        - `message`: contenido a publicar (bytes).

        Comportamiento:
        - Publica una vez por cada routing key configurada.
        - Si no hay routing keys, publica una vez con `''` teniendo el comportamiento de broadcast.
        """
        try:
            routing_keys = self.routing_keys if self.routing_keys else ['']
            for routing_key in routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)
        except AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ")

    def close(self):
        """Cierra la conexión de la instancia."""
        try:
            self.connection.close()
        except Exception:
            raise MessageMiddlewareCloseError("Failed to close the connection to RabbitMQ")

