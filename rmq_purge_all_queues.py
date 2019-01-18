import pika, os, logging, time, json, requests

logging.basicConfig()

def get_all_queues(host, user, passwd):
  print ("Call RabbitMQ api...")
  url = 'https://%s/api/queues' % (host)
  r = requests.get(url, auth=(user,passwd))
  return r

def get_queue_name(json_list):
  print ("Get queues names...")
  res = []
  for json in json_list:
    if("deadletter" not in json["name"]):
      res.append(json["name"])
  return res

def create_queue(host, user, passwd, vhost, queue):
  print ("Call RabbitMQ api...")
  url = 'https://{}/api/queues/{}/{}'.format(host, vhost, queue)
  print("Create Queue URL: {}".format(url))
  headers = {'Content-type': 'application/json'}
  data = {"auto_delete":False,"durable":True}
  print("Create Queue DATA: {}".format(data))
  r = requests.put(url, auth=(user,passwd), data=json.dumps(data), headers=headers)
  return r

def is_queue_exists(host, user, passwd, vhost, queue):
  print ("Call RabbitMQ api...")
  print("Verifying if queue {} exists...".format(queue))
  url = 'https://{}/api/queues/{}/{}'.format(host, vhost, queue)
  r = requests.get(url, auth=(user,passwd))
  return r.status_code == 200

def is_exchange_exists(host, user, passwd, vhost, exchange):
  print ("Call RabbitMQ api...")
  print("Verifying if exchange {} exists...".format(exchange))
  url = 'https://{}/api/exchanges/{}/{}'.format(host, vhost, exchange)
  r = requests.get(url, auth=(user,passwd))
  return r.status_code == 200

def create_exchange(host, user, passwd, vhost, exchange):
  print ("Call RabbitMQ api...")
  url = 'https://{}/api/exchanges/{}/{}'.format(host, vhost, exchange)
  print("Create Exchange URL: {}".format(url))
  headers = {'Content-type': 'application/json'}
  data = {"type":"direct","auto_delete":False,"durable":True}
  print("Create Exchange DATA: {}".format(data))
  r = requests.put(url, auth=(user,passwd), data=json.dumps(data), headers=headers)
  return r

def create_bindind(host, user, passwd, vhost, exchange, queue):
  print ("Call RabbitMQ api...")
  url = 'https://{}/api/bindings/{}/e/{}/q/{}'.format(host, vhost, exchange, queue)
  print("Create Binding URL: {}".format(url))
  headers = {'Content-type': 'application/json'}
  data = {"routing_key":queue}
  print("Create Binding DATA: {}".format(data))
  r = requests.post(url, auth=(user,passwd), data=json.dumps(data), headers=headers)
  return r


def set_queue_policy(host, user, passwd, vhost, queue, dlx, dlq):
  print ("Call RabbitMQ api...")
  url = 'https://{}/api/policies/{}/expiry-policy-{}'.format(host, vhost, queue)
  print("Set queue policy URL: {}".format(url))
  headers = {'Content-type': 'application/json'}
  data = {"pattern":"(^{})".format(queue), "definition": {"message-ttl":6000, "ha-mode":"all", "ha-sync-mode":"automatic", "dead-letter-exchange":dlx, "dead-letter-routing-key":dlq}, "priority":10, "apply-to": "queues"}
  print("Set queue policy DATA: {}".format(data))
  r = requests.put(url, auth=(user,passwd), data=json.dumps(data), headers=headers)
  return r

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
user = 'dqoyaazj'
passwd = 'lwBCAjY59jvmpxLEdHp5qHBTy9XOVKG0'
res = get_all_queues(host, user, passwd)
q_name = get_queue_name(res.json())

print ("Queues found: ")
print (q_name)

hashm = dict((json["name"], json["vhost"]) for json in res.json())
print (hashm)

for key in hashm:
  print (key)
  dlx = "deadletter.{}".format(hashm.get(key))
  
  exists = is_exchange_exists(host, user, passwd, hashm.get(key), dlx)
  if not exists:
    print("Dead leter exchange does not exist in the vhost {}. Creating...".format(hashm.get(key)))
    create_exchange(host, user, passwd, hashm.get(key), dlx)

filtered_dict = {k:v for (k,v) in hashm.items() if k in q_name}
print(filtered_dict)

for key, value in filtered_dict.items():
  print ("Purging messages from {} queue...", key)
  channel.queue_purge(queue=key)

  dlx = "deadletter.{}".format(value)
  dlq = "deadletter.{}".format(key)

  exists_queue = is_queue_exists(host, user, passwd, value, dlq)
  if not exists_queue:
    print("Create Dead Letter Queue {}....".format(dlq))
    queue_response = create_queue(host, user, passwd, value, dlq)
    print("Queue code: {}".format(queue_response))
    create_bindind(host, user, passwd, value, dlx, dlq)
      
    print("Set Messages TTL and Dead Letter Exchange policies for the queue {} in vhost {}...".format(key, value))
    policy_response = set_queue_policy(host, user, passwd, value, key, dlx, dlq)
    print("Policy code: {}".format(policy_response))

  print ("Done!")   

connection.close()
