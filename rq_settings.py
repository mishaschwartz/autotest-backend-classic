import os

queues = os.environ.get('QUEUES', 'high low batch').strip().split()
allowed_queues = ['high', 'low', 'batch']
assert all(q in allowed_queues for q in queues), f'Queue names must be one of: {allowed_queues}'

QUEUES = ['settings', *queues]
REDIS_URL = os.environ.get('REDIS_URL')
