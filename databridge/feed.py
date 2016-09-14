import gevent
import logging
import requests
from gevent.queue import Queue, Full
from gevent.event import Event
from .contrib.client import APICLient
from .contrib.retreive import RetreiverForward, RetreiverBackward
from .exceptions import LBMismatchError


QUEUE_FULL_DELAY = 5
EMPTY_QUEUE_DELAY = 1
ON_EMPTY_DELAY = 10
FORWARD_WORKER_SLEEP = 5
BACKWARD_WOKER_DELAY = 1
WATCH_DELAY = 1


logger = logging.getLogger(__name__)


class APIRetreiver(object):

    def __init__(self, config, **options):
        if not isinstance(config, dict):
            raise TypeError(
                "Expected a dict as config, got {}".format(type(config))
            )

        self.api_host = config.get('api_host')
        self.api_version = config.get('api_version')
        self.api_key = config.get('api_key')

        if 'api_extra_params' in options:
            self._extra = options.get('api_extra_params')

        self.tender_queue = Queue(maxsize=config.get('queue_max_size', 250))

        self.filter_callback = options.get('filter_callback', lambda x: x)

        self.forward_worker_dead = Event()
        self.forward_worker_dead.set()
        self.backward_worker_dead = Event()
        self.backward_worker_dead.set()
        if 'session' in options:
            self.session = options.pop('session')

        self._init_clients()

    def _init_clients(self):
        logger.info('Sync: Init clients')
        self.forward_client = APICLient(
            self.api_key,
            self.api_host,
            self.api_version,
            session=self.session
        )
        self.backward_client = APICLient(
            self.api_key,
            self.api_host,
            self.api_version,
            session=self.session
        )

        self.origin_cookie = self.forward_client.session.cookies
        self.backward_client.session.cookies = self.origin_cookie

    def _get_sync_point(self):
        logger.info('Sync: initializing sync')
        forward = {'feed': 'changes'}
        backward = {'feed': 'changes', 'descending': '1'}
        if getattr(self, '_extra', ''):
            [x.update(self._extra) for x in [forward, backward]]
        r = self.backward_client.get_tenders(backward)
        backward['offset'] = r['next_page']['offset']
        forward['offset'] = r['prev_page']['offset']
        logger.error(forward)
        self.tender_queue.put(filter(self.filter_callback, r['data']))
        logger.info('Sync: initial sync params forward: '
                    '{}, backward: {}'.format(forward, backward))
        return forward, backward

    def _start_sync_workers(self):
        forward, backward = self._get_sync_point()
        self.workers = [
            gevent.spawn(self._forward_worker, forward),
            gevent.spawn(self._backward_worker, backward),
        ]
        logger.debug('Started sync workers')

    def _restart_workers(self):
        self._init_clients()
        gevent.killall(self.workers)
        self._start_sync_workers()
        return self.workers

    def __iter__(self):
        self._start_sync_workers()
        forward, backward = self.workers
        try:
            while True:
                if self.tender_queue.empty():
                    gevent.sleep(EMPTY_QUEUE_DELAY)
                if forward.dead or forward.ready():
                    forward, backward = self._restart_workers()
                if (backward.dead or backward.ready()) and not backward.successful():
                    logger.info('Backward worker not active. restarting')
                    forward, backward = self._restart_workers()
                if backward.successful():
                    logger.info('Backward worker finished')
                while not self.tender_queue.empty():
                    yield self.tender_queue.get()
        except Exception as e:
            logger.error(e)
