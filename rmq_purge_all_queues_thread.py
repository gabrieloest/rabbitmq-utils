import functools
import logging
import pika
import threading
import time
import os
import requests

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

def call_rabbitmq_api(host, port, user, passwd):
  print ("Call RabbitMQ api...")
  url = 'https://%s/api/queues' % (host)
  r = requests.get(url, auth=(user,passwd))
  return r

def get_queue_name(json_list):
  print ("Get queues names...")
  res = []
  for json in json_list:
    res.append(json["name"])
  return res

# Parse CLODUAMQP_URL (fallback to localhost)
print ("Parse CLODUAMQP_URL (fallback to localhost)...")
url = os.environ.get('CLOUDAMQP_URL', 'amqp://dqoyaazj:lwBCAjY59jvmpxLEdHp5qHBTy9XOVKG0@shark.rmq.cloudamqp.com/dqoyaazj')
params = pika.URLParameters(url)
params.socket_timeout = 5

# Connect to CloudAMQP
print ("Connect to CloudAMQP...")
connection = pika.BlockingConnection(params)
channel = connection.channel() # start a channel

print ("Init params...")
host = 'shark.rmq.cloudamqp.com'
port = 55672
user = 'dqoyaazj'
passwd = 'lwBCAjY59jvmpxLEdHp5qHBTy9XOVKG0'
res = call_rabbitmq_api(host, port, user, passwd)
q_name = get_queue_name(res.json())

print ("Queues found: ")
print (q_name)

def ack_message(channel, delivery_tag):
    """Note that `channel` must be the same pika channel instance via which
    the message being ACKed was retrieved (AMQP protocol constraint).
    """
    LOGGER.info('Message akc...')
    if channel.is_open:
        channel.basic_ack(delivery_tag)
    else:
        # Channel is already closed, so we can't ACK this message;
        # log and/or do something that makes sense for your app in this case.
        pass

def do_work(connection, channel, delivery_tag, body):
    thread_id = threading.get_ident()
    fmt1 = 'Thread id: {} Delivery tag: {} Message body: {}'
    LOGGER.info(fmt1.format(thread_id, delivery_tag, body))

    cb = functools.partial(ack_message, channel, delivery_tag)
    connection.add_callback_threadsafe(cb)

def on_message(channel, method_frame, header_frame, body, args):
    (connection, threads) = args
    delivery_tag = method_frame.delivery_tag
    t = threading.Thread(target=do_work, args=(connection, channel, delivery_tag, body))
    t.start()
    threads.append(t)

threads = []
on_message_callback = functools.partial(on_message, args=(connection, threads))

for queue in q_name:
  print('Processing queue {}'.format(queue))
  channel.basic_consume(on_message_callback,
                      queue=queue)

try:
  LOGGER.info('Start consuming...')
  channel.start_consuming()
except KeyboardInterrupt:
  LOGGER.info('Stop consuming...')
  channel.stop_consuming()

# Wait for all to complete
LOGGER.info('Wait for all to complete...')
for thread in threads:
    thread.join()

connection.close()
