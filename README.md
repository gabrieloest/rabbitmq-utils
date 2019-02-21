
# RabbitMQ Utils

## About the project
This Project contains basic actions to execute on a RabbitMQ broker.
  * publisher.py - publish messages in a queue every second.
  * publisher_all.py - publish a specifi quantity of messages in every queue of the vhost configured.
  * consumer.py - listener a specific queue and consume the messages

## Configuration
1. Create file `config/server-config.yml` with the following content:
```yaml
rabbitmq:
  protocol:
  host:
  amqp-port:
  http-port:
  user:
  password:
  vhost:
```

## Usage
```console
foo@bar:~$ git clone https://github.com/gabrieloest/rabbitmq-utils
```
```console
foo@bar:~$ cd rabbitmq-utils
```
```console
foo@bar:~$ python -m pip install -r requirements.txt
```

### To run the `publisher.py` script
At cmd prompt:
```console
foo@bar:~$ python module/publisher.py
```
A message will appear:
```console
foo@bar:~$ Please enter queue name:
```
If the queue name is valid, the script start to publish messages into the selected queue. To stop, press `ctrl + c`
```console
INFO:__main__: [x] Message 1 sent to queue mapfilter
INFO:__main__: [x] Message 2 sent to queue mapfilter
INFO:__main__: [x] Message 3 sent to queue mapfilter
INFO:__main__: [x] Message 4 sent to queue mapfilter
INFO:__main__: [x] Message 5 sent to queue mapfilter
```

### To run the `publisher_all.py` script
At cmd prompt:
```console
foo@bar:~$ python module/publisher_all.py
```
A message will appear:
```console
foo@bar:~$ Please enter number of messages to send:
```
The script will send the amount of messages you choose and then stops.
```console
INFO:__main__: [x] Message 0 sent to queue dlqtransfer
INFO:__main__: [x] Message 0 sent to queue mapfilter
INFO:__main__: [x] Message 0 sent to queue pdfprocess
INFO:__main__: [x] Message 0 sent to queue purgetest
```

### To run the `consumer.py` script
At cmd prompt:
```console
foo@bar:~$ python module/publisher.py
```
A message will appear:
```console
foo@bar:~$ Please enter queue name:
```
If the queue name is valid, the script start to consume the messages from the selected queue. To stop, press `ctrl + c`
```console
Message 1 processing finished
Message ack OK!
Processing message...
Message 2 processing finished
Message ack OK!
Processing message...
Message 3 processing finished
Message ack OK!
Processing message...
Message 4 processing finished
Message ack OK!
Processing message...
Message 5 processing finished
Message ack OK!
```
