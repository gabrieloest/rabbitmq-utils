import pika
import os
import logging
import random

logging.basicConfig()

# Parse CLODUAMQP_URL (fallback to localhost)
url = os.environ.get(
    'CLOUDAMQP_URL', 'amqp://dqoyaazj:lwBCAjY59jvmpxLEdHp5qHBTy9XOVKG0@shark.rmq.cloudamqp.com/dqoyaazj')
params = pika.URLParameters(url)
params.socket_timeout = 5

# Connect to CloudAMQP
connection = pika.BlockingConnection(params)

# start a channel
channel = connection.channel()

queues_list = ['pdfprocess', 'purgetest', 'dlqtransfer']

for queue in queues_list:

    # Declare a queue
    channel.queue_declare(queue=queue)

    # send a message
    for number in range(500):
        channel.basic_publish(exchange='', routing_key=queue, body='Queue {} message number {}'.format(
            queue, random.randint(0, 100)*number))
        print(" [x] Message {} sent to queue {}".format(number, queue))
