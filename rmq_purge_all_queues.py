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
    res.append(json["name"])
  return res

def set_queue_policy(host, user, passwd, vhost, queue):
  print ("Call RabbitMQ api...")
  url = 'https://{}/api/policies/{}/expiry-policy-{}'.format(host, vhost, queue)
  print("Set queue policy URL: {}".format(url))
  headers = {'Content-type': 'application/json'}
  data = {"pattern":"(^{})".format(queue), "definition": {"message-ttl":6000, "ha-mode":"all", "ha-sync-mode":"automatic"}, "priority":10, "apply-to": "queues"}
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

queues = ["pdfprocess", "purgetest"]
for key, value in hashm.items():
    print ("Purging messages from {} queue...", key)
    channel.queue_purge(queue=key)
    print("Set Messages TTL policy for the queue {} in vhost {}...".format(key, value))
    response = set_queue_policy(host, user, passwd, value, key)
    print("Policy code: {}".format(response))
    print ("Done!")   

connection.close()
