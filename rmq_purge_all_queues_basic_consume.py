import pika, os, logging, time, json, requests

logging.basicConfig()

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

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    time.sleep(body.count(b'.'))
    print(" [x] Done")
    ch.basic_ack(delivery_tag = method.delivery_tag)


for queue in q_name:
  print('Processing queue {}'.format(queue))
  channel.basic_consume(callback,
                      queue=queue)

channel.start_consuming()


connection.close()
