import pika
import os
import logging
from itertools import groupby
import config_resolver
import rabbitmq_api_utils

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config = config_resolver.ConfigResolver(logger)
server_config = config.load_server_config()

logger.info("Parse URL (fallback to localhost)...")
url = os.environ.get('URL', 'amqp://{}:{}@{}:{}/{}'
                     .format(server_config['user'], server_config['password'],
                             server_config['host'], server_config['amqp-port'], server_config['vhost']))
logger.info(url)
params = pika.URLParameters(url)
params.socket_timeout = 5

# Connect to CloudAMQP
connection = pika.BlockingConnection(params)

# start a channel
channel = connection.channel()

rabbitmq_api_utils = rabbitmq_api_utils.RabbitmqAPIUtils(server_config['protocol'],
                                                         server_config['host'],
                                                         server_config['http-port'],
                                                         server_config['user'],
                                                         server_config['password'])

queues = list(rabbitmq_api_utils.get_all_queues().json())

print('Total number of queues: {}'.format(len(queues)))

queues_sorted_by_vhost = sorted(queues, key=lambda k: k['vhost'])

vhost_queues = dict((k, len(list(g))) for k, g in groupby(queues_sorted_by_vhost, lambda queue: queue['vhost']))


print('Number of queues by vhost: ')
for k, v in vhost_queues.items():
    print('{} - {}'.format(k, v))


queues_sorted_by_node = sorted(queues, key=lambda k: k['node'])

node_queues = dict((k, len(list(g))) for k, g in groupby(queues_sorted_by_node, lambda queue: queue['node']))

print('Number of queues by node: ')
for k, v in node_queues.items():
    print('{} - {}'.format(k, v))