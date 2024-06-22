import pika
import time

ALLOWED_TYPES = ('fanout', 'direct', 'topic', 'headers')
PREFETCH_COUNT = 200
AUTO_ACK_MODE = False
CONNECTION_TRIES = 10
LOOP_LAPSE_START = 2

class Middleware:

    def __init__(self, sigterm_queue):
        loop_lapse = LOOP_LAPSE_START
        connection = None
        for _ in range(CONNECTION_TRIES):
            if not sigterm_queue.empty():
                raise Exception('Sigterm received. Gracefully exiting...')

            try:
                time.sleep(loop_lapse)
                connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
                break
            except:
                loop_lapse = loop_lapse * 2
                
        if not connection:
            raise Exception('Connection to RabbitMQ failed')

        self._connection = connection
        self._channel = connection.channel()
        self._queues = set()
        self._exchanges = set()


    def _declare_exchange(self, name, type):
        if type not in ALLOWED_TYPES:
            raise Exception(f'Type {type} is not allowed')
        
        self._exchanges.add(name)
        self._channel.exchange_declare(exchange=name, exchange_type=type)

    def _declare_queue(self, name):
        self._queues.add(name)
        self._channel.queue_declare(queue=name)

    def publish_message(self, exchange, type, routing_key, message):
        if exchange not in self._exchanges:
            self._declare_exchange(exchange, type)
        
        self._channel.basic_publish(exchange=exchange, routing_key=routing_key, body=message)

    def send_message(self, queue, message):
        if queue not in self._queues:
            self._declare_queue(queue)
        
        self._channel.basic_publish(exchange='', routing_key=queue, body=message)
        
    def receive_messages(self, queue, callback, prefetch_count=PREFETCH_COUNT):
        if queue not in self._queues:
            self._declare_queue(queue)

        self._channel.basic_qos(prefetch_count=prefetch_count)
        self._channel.basic_consume(queue, callback, auto_ack=AUTO_ACK_MODE)

    def subscribe(self, exchange, queue, callback, prefetch_count=PREFETCH_COUNT):
        if exchange not in self._exchanges:
            raise Exception(f'Exchange {exchange} not defined before')
        elif queue not in self._queues:
            raise Exception(f'Queue {queue} not declared/binded before')
        
        self._channel.basic_qos(prefetch_count=prefetch_count)
        self._channel.basic_consume(queue, callback, auto_ack=AUTO_ACK_MODE)

    def define_exchange(self, exchange, queues_dict):
        self._declare_exchange(exchange, 'direct') # TODO: agregar esto a los parametros
        for queue, routing_keys in queues_dict.items():
            self._declare_queue(queue)
            for rk in routing_keys:
                self._channel.queue_bind(exchange=exchange, queue=queue, routing_key=rk)
    
    def ack_message(self, method):
        self._channel.basic_ack(delivery_tag=method.delivery_tag)

    def consume(self):
        self._channel.start_consuming()

    def ack_message(self, msg_identifier):
        """
        The user can pass the method or the delivery tag to ack the message
        """
        if isinstance(msg_identifier, int):
            self._channel.basic_ack(delivery_tag=msg_identifier)
        else:
            self._channel.basic_ack(delivery_tag=msg_identifier.delivery_tag)

    def close_connection(self):
        if self._channel.is_open:
            self._channel.stop_consuming()
            self._channel.close()
        self._connection.close()


    def stop_consuming(self, method=None):
        if method != None:
            self._channel.stop_consuming(consumer_tag=method.consumer_tag)
        else:    
            self._channel.stop_consuming()

    def ack_all(self, tag):
        self._channel.basic_ack(tag, True)

            
        