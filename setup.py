from distutils.core import setup

setup(name='rabbitmq-utils',
      version='1.0',
      description='Python scripts to manage RabbitMQ',
      author='Gabriel Oest',
      author_email='bieloest@gmail.com',
      url='https://github.com/gabrieloest/rabbitmq-utils',
      packages=['distutils', 'distutils.command', 'pika', 'requests', 'pyyaml']
      )
