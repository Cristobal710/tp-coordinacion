import pika
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange


class MessageMiddlewareRabbitMQBase:
    
    def _callback(self, channel, method, properties, body):
        def ack():
            channel.basic_ack(delivery_tag=method.delivery_tag)
        def nack():
            channel.basic_nack(delivery_tag=method.delivery_tag)
        self._on_message_callback(body, ack, nack)
    
    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        
    def close(self):
        try:
            self.connection.close()
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError() from e


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareRabbitMQBase, MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        try: 
            self.queue_name = queue_name
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            self.channel.basic_qos(prefetch_count=1)
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError) as e:
            raise MessageMiddlewareDisconnectedError() from e
        
    def start_consuming(self, on_message_callback):
        try:
            self._on_message_callback = on_message_callback
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=self._callback, auto_ack=False)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except pika.exceptions.AMQPChannelError as e:
            raise MessageMiddlewareMessageError() from e

    def send(self, message):
        try:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message,
                                   properties=pika.BasicProperties(delivery_mode= pika.DeliveryMode.Persistent))
        except pika.exceptions.AMQPConnectionError as e: 
            raise MessageMiddlewareDisconnectedError() from e
        except pika.exceptions.AMQPChannelError as e:
            raise MessageMiddlewareMessageError() from e

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareRabbitMQBase, MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        try:
            self.routing_keys = routing_keys
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.channel = self.connection.channel()
            self.exchange_name = exchange_name
            self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic')
            self.channel.basic_qos(prefetch_count=1)
            result = self.channel.queue_declare(queue='', exclusive=True)
            self.queue_name = result.method.queue
            for routing_key in self.routing_keys:
                self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=routing_key)
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError) as e:
            raise MessageMiddlewareDisconnectedError() from e
    
    def start_consuming(self, on_message_callback):
        try:
            self._on_message_callback = on_message_callback
            
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=self._callback, auto_ack=False)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except pika.exceptions.AMQPChannelError as e:
            raise MessageMiddlewareMessageError() from e

    def send(self, message):
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except pika.exceptions.AMQPChannelError as e:
            raise MessageMiddlewareMessageError() from e
    

