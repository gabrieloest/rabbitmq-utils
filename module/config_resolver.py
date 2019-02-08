import yaml

CONFIG_PATH = "./config/config.yml"


class ConfigResolver:

    def __init__(self, logger):
        self.log = logger

    def load_server_config(self):
        self.log.info('Loading server configurations....')
        with open(CONFIG_PATH, 'r') as ymlfile:
            server_config = yaml.load(ymlfile)

        rabbitmq = server_config['rabbitmq']
        host = rabbitmq['host']
        self.log.info('host: {}'.format(host))
        user = rabbitmq['user']
        self.log.info('user: {}'.format(user))
        password = rabbitmq['password']
        self.log.info('password: {}'.format(password))
        vhost = rabbitmq['vhost']
        self.log.info('vhost: {}'.format(vhost))

        server = dict()
        server['host'] = host
        server['user'] = user
        server['password'] = password
        server['vhost'] = vhost

        return server
