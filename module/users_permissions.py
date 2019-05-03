import logging
from itertools import groupby
import config_resolver
import rabbitmq_api_utils
import xlsxwriter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config = config_resolver.ConfigResolver(logger)
server_config = config.load_server_config()

rabbitmq_api_utils = rabbitmq_api_utils.RabbitmqAPIUtils(server_config['protocol'],
                                                         server_config['host'],
                                                         server_config['http-port'],
                                                         server_config['user'],
                                                         server_config['password'])

permissions = list(rabbitmq_api_utils.get_all_permissions().json())

permissions_sorted_by_user = sorted(permissions, key=lambda k: k['user'])

user_vhost = dict((k, list(map(lambda x: x['vhost'], g))) for k, g in groupby(permissions_sorted_by_user, lambda permission: permission['user']))

index = server_config['host'].index('.')
host_name = server_config['host'][0:index]

workbook = xlsxwriter.Workbook('users_permissions_{}.xlsx'.format(host_name))
worksheet = workbook.add_worksheet()

row = 0
col = 0

worksheet.write(row, col, 'User')
worksheet.set_column('A:A', 20)
worksheet.write(row, col+1, 'Vhost')
worksheet.set_column('B:B', 120)
worksheet.write(row, col+2, 'Name')
worksheet.set_column('C:C', 40)
worksheet.write(row, col+3, 'Email')
worksheet.set_column('D:D', 30)

row += 1
cell_format = workbook.add_format()
cell_format.set_text_wrap()
for k, v in user_vhost.items():
    print('{} - {}'.format(k, v))
    worksheet.write(row, col, k)
    worksheet.write(row, col + 1, str(v), cell_format)
    row += 1

workbook.close()