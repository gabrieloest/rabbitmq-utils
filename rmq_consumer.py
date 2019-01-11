import pika, os, logging, time

logging.basicConfig()

def pdf_process_function(msg):
    print "PDF processing"
    time.sleep(5) # delays for 5 seconds
    print "PDF processing finished";
    return;

# Parse CLODUAMQP_URL (fallback to localhost)
url = os.environ.get('CLOUDAMQP_URL', 'amqp://dqoyaazj:lwBCAjY59jvmpxLEdHp5qHBTy9XOVKG0@shark.rmq.cloudamqp.com/dqoyaazj')
params = pika.URLParameters(url)
params.socket_timeout = 5

# Connect to CloudAMQP
connection = pika.BlockingConnection(params)
channel = connection.channel() # start a channel

# create a function which is called on incoming messages
def callback(ch, method, properties, body):
    pdf_process_function(body)

#set up subscription on the queue
channel.basic_consume(callback,
queue='pdfprocess',
no_ack=True)

# start consuming (blocks)
channel.start_consuming()
connection.close()
