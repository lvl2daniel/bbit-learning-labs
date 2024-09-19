from producer_interface import mqProducerInterface

import pika
import os
import sys

class mqProducer (mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str) -> None:
        # Save parameters to instance variable
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        connection = pika.BlockingConnection(parameters=conParams)
        channel = connection.channel()
        channel.exchange_declare(self.exchange_name, exchange_type='direct')
        

    def publishOrder(self, message: str) -> None:
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        connection = pika.BlockingConnection(parameters=conParams)
        channel = connection.channel()
        channel.exchange_declare(self.exchange_name, exchange_type='direct')

        # Basic Publish to Exchange
        channel.basic_publish(exchange=self.exchange_name, routing_key=self.routing_key, body=message)

        # Close Channel
        channel.close()

        # Close Connection
        connection.close()
        

        
