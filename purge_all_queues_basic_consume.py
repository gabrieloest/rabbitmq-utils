import pika
import os
import logging
import yaml
import rabbitmq_api_utils

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info('Loading configurations....')
with open("config.yml", 'r') as ymlfile:
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
logger.info("Connect to CloudAMQP...")
connection = pika.BlockingConnection(params)
channel = connection.channel()  # start a channel


rmq_utils = rabbitmq_api_utils.RabbitmqAPIUtils(host, user, password)

all_queues = rmq_utils.get_all_queues()
queues_names = rmq_utils.get_queue_name(all_queues.json())

logger.info("Queues found: ")
logger.info(queues_names)

queue_name_vhost = dict((json["name"], json["vhost"])
                        for json in all_queues.json())
logger.info(queue_name_vhost)
queue_name_vhost = {
    k: v for (k, v) in queue_name_vhost.items() if k in queues_names}

for key in queue_name_vhost:
    logger.info(key)
    dead_letter_exchange = "deadletter.{}".format(queue_name_vhost.get(key))

    exists = rmq_utils.is_exchange_exists(
        queue_name_vhost.get(key), dead_letter_exchange)
    if not exists:
        logger.info('Dead leter exchange does not exist in the vhost {}. Creating...'.format(
            queue_name_vhost.get(key)))
        rmq_utils.create_exchange(
            queue_name_vhost.get(key), dead_letter_exchange)


class CountCallback(object):
    def __init__(self, count, exchange, routing_key):
        self.count = count
        self.exchange = exchange
        self.routing_key = routing_key

    def __call__(self, channel, method, properties, body):
        channel.basic_publish(
            exchange='', routing_key=self.routing_key, body=body)
        print("")
        self.count -= 1
        if not self.count:
            channel.stop_consuming()


for key, value in queue_name_vhost.items():
    dead_letter_exchange = "deadletter.{}".format(value)
    dead_letter_queue = "deadletter.{}".format(key)

    exists_queue = rmq_utils.is_queue_exists(value, dead_letter_queue)
    if not exists_queue:
        logger.info("Create Dead Letter Queue {}....".format(
            dead_letter_queue))
        queue_response = rmq_utils.create_queue(value, dead_letter_queue)
        logger.info("Queue code: {}".format(queue_response))
        rmq_utils.create_bindind(
            value, dead_letter_exchange, dead_letter_queue)

        logger.info(
            "Set Messages TTL and Dead Letter Exchange policies for the queue {} in vhost {}...".format(key, value))
        policy_response = rmq_utils.create_queue_policy(
            value, key, dead_letter_exchange, dead_letter_queue)
        logger.info("Policy code: {}".format(policy_response))

    print('Processing queue {}'.format(key))
    queue = channel.queue_declare(key)
    callback = CountCallback(queue.method.message_count,
                             dead_letter_exchange, dead_letter_queue)
    channel.basic_consume(callback, queue=key)
    channel.start_consuming()

    logger.info("Done!")

channel.close()
connection.close()
