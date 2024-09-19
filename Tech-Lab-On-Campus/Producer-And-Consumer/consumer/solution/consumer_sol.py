from consumer_interface import mqConsumerInterface
import pika
import os

# Create a class named mqConsumer

class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_key: str, exchange_name: str, queue_name: str) -> None:
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.setupRMQConnection()


    # setupRMQConnection Function: 
    # Establish connection to the RabbitMQ service, 
    # declare a queue and exchange,
    # bind the binding key to the queue on the exchange
    # finally set up a callback function for receiving messages
    def setupRMQConnection(self) -> None:
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        connection = pika.BlockingConnection(parameters=conParams)
        channel = connection.channel()
        channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')
        channel.queue_declare(queue=self.queue_name, durable=True)
        channel.queue_bind(exchange=self.exchange_name, routing_key=self.binding_key, queue=self.queue_name)
        channel.basic_consume(queue=self.queue_name, on_message_callback=self.on_message_callback, auto_ack=True)

    # Print the UTF-8 string message and then close the connection.
    def on_message_callback(self, channel, method_frame, header_frame, body) -> None:
        # Acknowledge message
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        print(f"{body.decode()}")
        self.__del__()

    def __del__(self) -> None:
        print("Closing RMQ connection on destruction")
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        connection = pika.BlockingConnection(parameters=conParams)
        channel = connection.channel()
        channel.close()
        connection.close()

    def startConsuming(self) -> None:
        print(" [*] Waiting for messages. To exit press CTRL+C")
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        connection = pika.BlockingConnection(parameters=conParams)
        connection.process_data_events()
        connection.close()
        os._exit(0)
        
    
    
        