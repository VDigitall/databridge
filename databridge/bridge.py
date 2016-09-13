import gevent
import logging
import requests
from gevent.queue import Queue, Full
from .client import APICLient


QUEUE_FULL_DELAY = 5

logger = logging.getLogger(__name__)


class APIDataBridge(object):

    def __init__(self, config):
        if not isinstance(config, dict):
            raise TypeError(
                "Expected a dict as config, got {}".format(type(config))
            )

        self.api_host = config.get('api_host')
        self.api_version = config.get('api_version')
        self.api_key = config.get('api_key')
        self.tender_queue = Queue(maxsize=config.get('queue_max_size', 500))

    def _init_clients(self):
        self.forward_client = APICLient(
            self.api_key,
            self.api_host,
            self.api_version
        )
        self.backward_client = APICLient(
            self.api_key,
            self.api_host,
            self.api_version
        )
        self.origin_cookie = self.forward_client.session.cookies
        self.backward_client.session.cookies = self.origin_cookie

    def _init_syncronization(self):
        self._init_clients()

        forward = {'feed': 'chages'}
        backward = {'feed': 'changes', 'descending': True}
        try:
            r = self.backward_client.get_tenders(params=backward)
        except requests.exceptions.RequestException as e:
            logger.error('Error on initiaziong')

        backward['offset'] = r['next_page']['offset']
        forward['offset'] = r['prev_page']['offset']
        self.tender_queue.put(r['data'])
        self._start_sync_workers()

    def _start_sync_workers(self):

