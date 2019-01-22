import pika, os, logging, time, json
import rabbitmq_api_utils

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Init params...")
host = 'shark.rmq.cloudamqp.com'
user = 'dqoyaazj'
password = 'lwBCAjY59jvmpxLEdHp5qHBTy9XOVKG0'

# Parse CLODUAMQP_URL (fallback to localhost)
logger.info("Parse CLODUAMQP_URL (fallback to localhost)...")
url = os.environ.get('CLOUDAMQP_URL', 'amqp://{}:{}@{}/dqoyaazj'.format(user, password, host))
params = pika.URLParameters(url)
params.socket_timeout = 5

# Connect to CloudAMQP
logger.info("Connect to CloudAMQP...")
connection = pika.BlockingConnection(params)
channel = connection.channel() # start a channel



rmq_utils = rabbitmq_api_utils.RabbitmqAPIUtils(host, user, password)

all_queues = rmq_utils.get_all_queues()
queues_names = rmq_utils.get_queue_name(all_queues.json())

logger.info("Queues found: ")
logger.info(queues_names)

queue_name_vhost = dict((json["name"], json["vhost"]) for json in all_queues.json())
logger.info(queue_name_vhost)
queue_name_vhost = {k:v for (k,v) in queue_name_vhost.items() if k in queues_names}

for key in queue_name_vhost:
  logger.info(key)
  dead_letter_exchange = "deadletter.{}".format(queue_name_vhost.get(key))
  
  exists = rmq_utils.is_exchange_exists(queue_name_vhost.get(key), dead_letter_exchange)
  if not exists:
    logger.info("Dead leter exchange does not exist in the vhost {}. Creating...".format(queue_name_vhost.get(key)))
    rmq_utils.create_exchange(queue_name_vhost.get(key), dead_letter_exchange)


for key, value in queue_name_vhost.items():
  logger.info("Purging messages from {} queue...".format(key))
  channel.queue_purge(queue=key)

  dead_letter_exchange = "deadletter.{}".format(value)
  dead_letter_queue = "deadletter.{}".format(key)

  exists_queue = rmq_utils.is_queue_exists(value, dead_letter_queue)
  if not exists_queue:
    logger.info("Create Dead Letter Queue {}....".format(dead_letter_queue))
    queue_response = rmq_utils.create_queue(value, dead_letter_queue)
    logger.info("Queue code: {}".format(queue_response))
    rmq_utils.create_bindind(value, dead_letter_exchange, dead_letter_queue)
      
    logger.info("Set Messages TTL and Dead Letter Exchange policies for the queue {} in vhost {}...".format(key, value))
    policy_response = rmq_utils.create_queue_policy(value, key, dead_letter_exchange, dead_letter_queue)
    logger.info("Policy code: {}".format(policy_response))

  logger.info("Done!")   

channel.close()
connection.close()
