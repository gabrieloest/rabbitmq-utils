import pika
import os
import logging
import random
import config_resolver
import rabbitmq_api_utils

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config = config_resolver.ConfigResolver(logger)

server_config = config.load_server_config()

logger.info("Parse CLODUAMQP_URL (fallback to localhost)...")
url = os.environ.get('CLOUDAMQP_URL', 'amqp://{}:{}@{}/{}'
                     .format(server_config['user'], server_config['password'],
                             server_config['host'], server_config['vhost']))
params = pika.URLParameters(url)
params.socket_timeout = 5

# Connect to CloudAMQP
connection = pika.BlockingConnection(params)

# start a channel
channel = connection.channel()

rabbitmq_api_utils = rabbitmq_api_utils.RabbitmqAPIUtils(server_config['protocol'],
                                                         server_config['host'],
                                                         server_config['user'],
                                                         server_config['password'])

response = rabbitmq_api_utils.get_all_queues_by_vhost(server_config['vhost'])

if(response.status_code is not 200):
    logger.error('Queue not found!')
    exit()

queues = response.json()

number_of_messages = int(input("Please enter number of messages to send: "))

for queue in queues:

    # Declare a queue
    channel.queue_declare(queue=queue['name'], durable=queue['durable'])

    # send a message
    for number in range(number_of_messages):
        channel.basic_publish(exchange='', routing_key=queue['name'],
                              body='Queue {} message number {}'
                                   .format(queue['name'],
                                           random.randint(0, 100)*number))
        logger.info(" [x] Message {} sent to queue {}".format(number, queue['name']))
