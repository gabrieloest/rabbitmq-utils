import pika
import os
import logging
import random
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info('Loading configurations....')
with open("./config/config.yml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

rabbitmq = cfg['rabbitmq']
host = rabbitmq['host']
user = rabbitmq['user']
password = rabbitmq['password']

logger.info('host: {}'.format(host))
logger.info('user: {}'.format(user))
logger.info('password: {}'.format(password))

# Parse CLODUAMQP_URL (fallback to localhost)
logger.info("Parse CLODUAMQP_URL (fallback to localhost)...")
url = os.environ.get(
    'CLOUDAMQP_URL', 'amqp://{}:{}@{}/dqoyaazj'.format(user, password, host))
params = pika.URLParameters(url)
params.socket_timeout = 5

# Connect to CloudAMQP
connection = pika.BlockingConnection(params)

# start a channel
channel = connection.channel()

queue = input("Please enter queue name: ")
number_of_messages = int(
    input("Please enter how many messages you want to send: "))

# Declare a queue
channel.queue_declare(queue=queue)

# send a message

for number in range(number_of_messages):
    channel.basic_publish(exchange='', routing_key=queue, body='Queue {} message number {}'.format(
        queue, random.randint(0, 100)*number))
    print(" [x] Message {} sent to queue {}".format(number, queue))
