import pika
import os
import logging
import time
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


def pdf_process_function(msg):
    print("PDF processing")
    time.sleep(5)  # delays for 5 seconds
    print("PDF processing finished")
    return


# Connect to CloudAMQP
connection = pika.BlockingConnection(params)
channel = connection.channel()  # start a channel

# create a function which is called on incoming messages


def callback(ch, method, properties, body):
    pdf_process_function(body)


# set up subscription on the queue
channel.basic_consume(callback,
                      queue='pdfprocess',
                      no_ack=True)

# start consuming (blocks)
channel.start_consuming()
connection.close()
