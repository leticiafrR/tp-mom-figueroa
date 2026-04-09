import pika
from pika.exceptions import AMQPConnectionError, AMQPError
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
        self.queue_name = queue_name
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=queue_name)
        except AMQPConnectionError as error:
            raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ") from error
        except Exception as error:
            raise MessageMiddlewareMessageError(
                f"Failed to initialize queue middleware for '{queue_name}': {str(error)}"
            ) from error

    def _ensure_open(self):
        if not self.connection.is_open or not self.channel.is_open:
            raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ")

    def send(self, message):
        """
        Envía un mensaje a la cola configurada.

        Parámetros:
        - `message`: contenido a publicar (bytes).
        """
        try:
            self._ensure_open()
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        except MessageMiddlewareDisconnectedError:
            raise
        except AMQPConnectionError as error:
            raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ") from error
        except Exception as error:
            raise MessageMiddlewareMessageError(
                f"Failed to send message to queue '{self.queue_name}': {str(error)}"
            ) from error

    def start_consuming(self, on_message_callback):
        """
        Inicia el consumo bloqueante de mensajes.

        Parámetros:
        - `on_message_callback`: función con firma
          `callback(message, ack, nack)`.
        """

        def callback(ch, method, properties, body):
            try:
                self._ensure_open()

                def ack():
                    try:
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                    except AMQPError as error:
                        raise MessageMiddlewareMessageError(f"Failed to ack message: {str(error)}") from error

                def nack():
                    try:
                        ch.basic_nack(delivery_tag=method.delivery_tag)
                    except AMQPError as error:
                        raise MessageMiddlewareMessageError(f"Failed to nack message: {str(error)}") from error

                on_message_callback(
                    body,
                    ack,
                    nack,
                )
            except MessageMiddlewareDisconnectedError:
                raise
            except Exception as e:
                raise MessageMiddlewareMessageError(
                    f"An error occurred while processing the message: {str(e)}"
                )

        try:
            self._ensure_open()
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=False)
            self.channel.start_consuming()
        except MessageMiddlewareDisconnectedError:
            raise
        except AMQPConnectionError as error:
            raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ") from error
        except Exception as error:
            raise MessageMiddlewareMessageError(f"Failed to consume queue '{self.queue_name}': {str(error)}") from error

    def stop_consuming(self):
        """Detiene el consumo iniciado con `start_consuming()`."""
        try:
            self._ensure_open()
            self.channel.stop_consuming()
        except MessageMiddlewareDisconnectedError:
            raise
        except Exception as error:
            raise MessageMiddlewareMessageError(f"Failed to stop queue consumer: {str(error)}") from error

    def close(self):
        """Cierra la conexión de la instancia."""
        try:
            if getattr(self, "channel", None) and self.channel.is_open:
                self.channel.close()
            if getattr(self, "connection", None) and self.connection.is_open:
                self.connection.close()
        except Exception as error:
            raise MessageMiddlewareCloseError("Failed to close the connection to RabbitMQ") from error


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

    _FANOUT_ROUTING_KEY = "__mm_broadcast__"

    def __init__(self, host, exchange_name, routing_keys):
        """
        Crea una instancia asociada a un exchange y una lista de routing keys.

        Parámetros:
        - `host`: host/IP de RabbitMQ.
        - `exchange_name`: nombre del exchange.
        - `routing_keys`: routing keys de suscripción/publicación.
        """
        self.exchange_name = exchange_name
        if isinstance(routing_keys, str):
            self.routing_keys = [routing_keys]
        else:
            self.routing_keys = routing_keys or []

        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
        except AMQPConnectionError as error:
            raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ") from error
        except Exception as error:
            raise MessageMiddlewareMessageError(
                f"Failed to initialize exchange middleware for '{exchange_name}': {str(error)}"
            ) from error

    def _ensure_open(self):
        if not self.connection.is_open or not self.channel.is_open:
            raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ")

    def start_consuming(self, on_message_callback):
        """
        Inicia el consumo bloqueante de mensajes del exchange.

        Parámetros:
        - `on_message_callback`: función con firma
          `callback(message, ack, nack)`.
        """
        try:
            self._ensure_open()

            result = self.channel.queue_declare(queue='', exclusive=True, auto_delete=True)
            self.queue_name = result.method.queue

            for routing_key in self.routing_keys:
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=self.queue_name,
                    routing_key=routing_key,
                )

            self.channel.queue_bind(
                exchange=self.exchange_name,
                queue=self.queue_name,
                routing_key=self._FANOUT_ROUTING_KEY,
            )
        except MessageMiddlewareDisconnectedError:
            raise
        except AMQPConnectionError as error:
            raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ") from error
        except Exception as error:
            raise MessageMiddlewareMessageError(
                f"Failed to prepare consumer for exchange '{self.exchange_name}': {str(error)}"
            ) from error

        def callback(ch, method, properties, body):
            try:
                self._ensure_open()

                def ack():
                    try:
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                    except AMQPError as error:
                        raise MessageMiddlewareMessageError(f"Failed to ack message: {str(error)}") from error

                def nack():
                    try:
                        ch.basic_nack(delivery_tag=method.delivery_tag)
                    except AMQPError as error:
                        raise MessageMiddlewareMessageError(f"Failed to nack message: {str(error)}") from error

                on_message_callback(
                    body,
                    ack,
                    nack,
                )
            except MessageMiddlewareDisconnectedError:
                raise
            except Exception as e:
                raise MessageMiddlewareMessageError(
                    f"An error occurred while processing the message: {str(e)}"
                )

        try:
            self._ensure_open()
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=False)
            self.channel.start_consuming()
        except MessageMiddlewareDisconnectedError:
            raise
        except AMQPConnectionError as error:
            raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ") from error
        except Exception as error:
            raise MessageMiddlewareMessageError(
                f"Failed to consume exchange '{self.exchange_name}': {str(error)}"
            ) from error

    def stop_consuming(self):
        """Detiene el loop de consumo iniciado con `start_consuming()`."""
        try:
            self._ensure_open()
            self.channel.stop_consuming()
        except MessageMiddlewareDisconnectedError:
            raise
        except Exception as error:
            raise MessageMiddlewareMessageError(f"Failed to stop exchange consumer: {str(error)}") from error

    def send(self, message):
        """
        Publica el mensaje en el exchange.

        Parámetros:
        - `message`: contenido a publicar (bytes).

        Comportamiento:
        - Publica una vez por cada routing key configurada.
        - Si no hay routing keys, publica una sola vez con una routing key interna
          de broadcast para que todos los consumidores del exchange la reciban.
        """
        try:
            self._ensure_open()
            routing_keys = self.routing_keys if self.routing_keys else [self._FANOUT_ROUTING_KEY]
            for routing_key in routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)
        except MessageMiddlewareDisconnectedError:
            raise
        except AMQPConnectionError as error:
            raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ") from error
        except Exception as error:
            raise MessageMiddlewareMessageError(
                f"Failed to send message to exchange '{self.exchange_name}': {str(error)}"
            ) from error

    def close(self):
        """Cierra la conexión de la instancia."""
        try:
            if getattr(self, "channel", None) and self.channel.is_open:
                self.channel.close()
            if getattr(self, "connection", None) and self.connection.is_open:
                self.connection.close()
        except Exception as error:
            raise MessageMiddlewareCloseError("Failed to close the connection to RabbitMQ") from error
