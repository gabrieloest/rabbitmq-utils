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

res = rmq_utils.get_all_queues()
q_name = rmq_utils.get_queue_name(res.json())

logger.info("Queues found: ")
logger.info(q_name)

hashm = dict((json["name"], json["vhost"]) for json in res.json())
logger.info(hashm)
filtered_dict = {k:v for (k,v) in hashm.items() if k in q_name}

for key in filtered_dict:
  logger.info(key)
  dlx = "deadletter.{}".format(filtered_dict.get(key))
  
  exists = rmq_utils.is_exchange_exists(filtered_dict.get(key), dlx)
  if not exists:
    logger.info("Dead leter exchange does not exist in the vhost {}. Creating...".format(filtered_dict.get(key)))
    rmq_utils.create_exchange(filtered_dict.get(key), dlx)


for key, value in filtered_dict.items():
  logger.info("Purging messages from {} queue...".format(key))
  channel.queue_purge(queue=key)

  dlx = "deadletter.{}".format(value)
  dlq = "deadletter.{}".format(key)

  exists_queue = rmq_utils.is_queue_exists(value, dlq)
  if not exists_queue:
    logger.info("Create Dead Letter Queue {}....".format(dlq))
    queue_response = rmq_utils.create_queue(value, dlq)
    logger.info("Queue code: {}".format(queue_response))
    rmq_utils.create_bindind(value, dlx, dlq)
      
    logger.info("Set Messages TTL and Dead Letter Exchange policies for the queue {} in vhost {}...".format(key, value))
    policy_response = rmq_utils.create_queue_policy(value, key, dlx, dlq)
    logger.info("Policy code: {}".format(policy_response))

  logger.info("Done!")   

channel.close()
connection.close()
