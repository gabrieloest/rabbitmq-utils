import logging, json, requests

class RabbitmqAPIUtils

    headers = {'Content-type': 'application/json'}

    def __init__(self, user, password, host):
        self.user = user
        self.password = password
        self.url = 'https://%s/api/' % (host)
        logging.basicConfig()

    def get_all_queues(user, passwd):
      print ("Call RabbitMQ api...")
      url_method = url.join('queues')
      r = requests.get(url_method, auth=(self.user, self.password))
      return r

    def get_queue_name(json_list):
      print ("Get queues names...")
      res = []
      for json in json_list:
        if("deadletter" not in json["name"]):
          res.append(json["name"])
      return res

    def create_queue(vhost, queue):
      print ("Call RabbitMQ api...")
      url_method = url.join('queues/{}/{}'.format(vhost, queue))
      print("Create Queue URL: {}".format(url_method))      
      data = {"auto_delete":False,"durable":True}
      print("Create Queue DATA: {}".format(data))
      r = requests.put(url_method, auth=(self.user, self.password), data=json.dumps(data), headers=headers)
      return r

    def is_queue_exists(vhost, queue):
      print ("Call RabbitMQ api...")
      print("Verifying if queue {} exists...".format(queue))
      url_method = self.url.join('queues/{}/{}'.format(vhost, queue))
      r = requests.get(url_method, auth=(self.user, self.password))
      return r.status_code == 200

    def is_exchange_exists(vhost, exchange):
      print ("Call RabbitMQ api...")
      print("Verifying if exchange {} exists...".format(exchange))
      url_method = self.url.join('exchanges/{}/{}'.format(vhost, exchange))
      r = requests.get(url_method, auth=(self.user, self.password))
      return r.status_code == 200

    def create_exchange(vhost, exchange):
      print ("Call RabbitMQ api...")
      url_method = self.url.join('exchanges/{}/{}'.format(vhost, exchange))
      print("Create Exchange URL: {}".format(url_method))
      headers = {'Content-type': 'application/json'}
      data = {"type":"direct","auto_delete":False,"durable":True}
      print("Create Exchange DATA: {}".format(data))
      r = requests.put(url_method, auth=(self.user, self.password), data=json.dumps(data), headers=headers)
      return r

    def create_binding(vhost, exchange, queue):
      print ("Call RabbitMQ api...")
      url_method = self.url.join('bindings/{}/e/{}/q/{}'.format(vhost, exchange, queue))
      print("Create Binding URL: {}".format(url_method))
      headers = {'Content-type': 'application/json'}
      data = {"routing_key":queue}
      print("Create Binding DATA: {}".format(data))
      r = requests.post(url_method, auth=(self.user, self.password), data=json.dumps(data), headers=headers)
      return r

    def create_queue_policy(vhost, queue, dlx, dlq):
      print ("Call RabbitMQ api...")
      url_method = self.url.join('policies/{}/expiry-policy-{}'.format(vhost, queue))
      print("Set queue policy URL: {}".format(url_method))
      headers = {'Content-type': 'application/json'}
      data = {"pattern":"(^{})".format(queue), "definition": {"message-ttl":6000, "ha-mode":"all", "ha-sync-mode":"automatic", "dead-letter-exchange":dlx, "dead-letter-routing-key":dlq}, "priority":10, "apply-to": "queues"}
      print("Set queue policy DATA: {}".format(data))
      r = requests.put(url_method, auth=(self.user, self.password), data = json.dumps(data), headers=headers)
      return r
