import pika, os, logging, random

logging.basicConfig()

# Parse CLODUAMQP_URL (fallback to localhost)
url = os.environ.get('CLOUDAMQP_URL', 'amqp://dqoyaazj:lwBCAjY59jvmpxLEdHp5qHBTy9XOVKG0@shark.rmq.cloudamqp.com/dqoyaazj')
params = pika.URLParameters(url)
params.socket_timeout = 5

# Connect to CloudAMQP
connection = pika.BlockingConnection(params)

# start a channel
channel = connection.channel()

# Declare a queue
channel.queue_declare(queue='purgetest')

# send a message

for number in range(500):
    channel.basic_publish(exchange='', routing_key='purgetest', body='Purge test queue {}'.format(random.randint(0, 100)*number))
    print (" [x] Message sent to consumer{}".format(number))
    
