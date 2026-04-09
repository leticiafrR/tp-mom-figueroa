from os import close

import pika
from pika.exceptions import (
    AMQPConnectionError,
    AMQPError,
    ChannelClosed,
    ChannelClosedByBroker,
    ChannelClosedByClient,
    ChannelWrongStateError,
    ConnectionClosed,
    ConnectionClosedByBroker,
    ConnectionClosedByClient,
    ConnectionWrongStateError,
    StreamLostError,
)
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError
)


_DISCONNECTED_ERRORS = (
    AMQPConnectionError,
    AMQPError,
    ChannelClosed,
    ChannelClosedByBroker,
    ChannelClosedByClient,
    ChannelWrongStateError,
    ConnectionClosed,
    ConnectionClosedByBroker,
    ConnectionClosedByClient,
    ConnectionWrongStateError,
    StreamLostError,
)


def _is_disconnected_error(error):
    return isinstance(error, _DISCONNECTED_ERRORS)


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

    def send(self, message):
        """
        Envía un mensaje a la cola configurada.

        Parámetros:
        - `message`: contenido a publicar (bytes).
        """
        try:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        except Exception as error:
            if _is_disconnected_error(error):
                raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ") from error
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
            def ack():
                try:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as error:
                    if _is_disconnected_error(error):
                        raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ") from error
                    raise MessageMiddlewareMessageError(f"Failed to ack message: {str(error)}") from error

            def nack():
                try:
                    ch.basic_nack(delivery_tag=method.delivery_tag)
                except Exception as error:
                    if _is_disconnected_error(error):
                        raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ") from error
                    raise MessageMiddlewareMessageError(f"Failed to nack message: {str(error)}") from error

            try:
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
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=False)
            self.channel.start_consuming()
        except MessageMiddlewareDisconnectedError:
            raise
        except Exception as error:
            if _is_disconnected_error(error):
                raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ") from error
            raise MessageMiddlewareMessageError(f"Failed to consume queue '{self.queue_name}': {str(error)}") from error

    def stop_consuming(self):
        """Detiene el consumo iniciado con `start_consuming()`."""
        try:
            self.channel.stop_consuming()
        except MessageMiddlewareDisconnectedError:
            raise
        except Exception as error:
            if _is_disconnected_error(error):
                raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ") from error
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
    def __init__(self, host, exchange_name, routing_keys):
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.__call_function_with_error_mapping(self.__stablish_connection, host, exchange_name)
    
    
    @staticmethod
    def __call_function_with_error_mapping(func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (ChannelClosed, ChannelClosedByBroker, ChannelClosedByClient, ChannelWrongStateError,
                ConnectionClosed, ConnectionClosedByBroker, ConnectionClosedByClient, ConnectionWrongStateError) as error:
            raise MessageMiddlewareCloseError("RabbitMQ channel or connection was closed") from error
        except (AMQPConnectionError, StreamLostError) as error:
            raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ") from error
        except AMQPError as error:
            raise MessageMiddlewareMessageError(
                f"Failed to execute RabbitMQ operation: {str(error)}"
            ) from error


    def start_consuming(self, on_message_callback):
        self.__call_function_with_error_mapping(self.__reserve_receiver_resources, on_message_callback)
        self.__call_function_with_error_mapping(self.channel.start_consuming)


    def stop_consuming(self):
        self.__call_function_with_error_mapping(self.channel.stop_consuming)


    def send(self, message):
        for routing_key in self.routing_keys:
            self.__call_function_with_error_mapping(
                lambda: self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message,
                )
            )


    def close(self):
        self.__call_function_with_error_mapping(lambda: self.connection.close())


    def __stablish_connection(self, host, exchange_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')


    def __bind_all_routing_keys(self ):
        for routing_key in self.routing_keys:
            self.channel.queue_bind(
                exchange=self.exchange_name,
                queue=self.queue_name,
                routing_key=routing_key,
            )


    def __reserve_receiver_resources(self, on_message_callback):
        def callback(ch, method, properties, body):
            on_message_callback(
                body,
                lambda: self.__call_function_with_error_mapping(lambda: ch.basic_ack(delivery_tag=method.delivery_tag)),
                lambda: self.__call_function_with_error_mapping(lambda: ch.basic_nack(delivery_tag=method.delivery_tag)),
            )
        result = self.channel.queue_declare(queue='', exclusive=True, auto_delete=True)
        self.queue_name = result.method.queue
        self.__bind_all_routing_keys()
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=False)
