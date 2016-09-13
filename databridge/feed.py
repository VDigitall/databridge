import gevent
import logging
import requests
from gevent.queue import Queue, Full
from gevent.event import Event
from .client import APICLient
from .exceptions import LBMismatchError


QUEUE_FULL_DELAY = 5
EMPTY_QUEUE_DELAY = 1
ON_EMPTY_DELAY = 10
FORWARD_WORKER_SLEEP = 5
BACKWARD_WOKER_DELAY = 1
WATCH_DELAY = 1


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


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

        self._init_clients()

    def _init_clients(self):
        logger.info('Sync: Init clients')
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

    def _get_sync_point(self):
        logger.info('Sync: initializing sync')
        forward = {'feed': 'changes'}
        backward = {'feed': 'changes', 'descending': '1'}
        if getattr(self, '_extra', ''):
            [x.update(self._extra) for x in [forward, backward]]
        r = self.backward_client.get_tenders(backward)
        backward['offset'] = r['next_page']['offset']
        forward['offset'] = r['prev_page']['offset']
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

    def _forward_worker(self, params):
        worker = "Forward worker: "
        logger.info('{} starting'.format(worker))
        r = self.forward_client.get_tenders(params)
        if self.forward_client.session.cookies != self.origin_cookie:
            raise LBMismatchError

        try:
            while True:
                try:
                    while r['data']:
                        try:
                            self.tender_queue.put(
                                filter(self.filter_callback, r['data'])
                            )
                        except Full:
                            while self.tender_queue.full():
                                gevent.sleep(QUEUE_FULL_DELAY)
                                self.tender_queue.put(
                                    filter(self.filter_callback, r['data'])
                                )
                        params['offset'] = r['next_page']['offset']

                        r = self.forward_client.get_tender(params)
                        if self.forward_client.session.cookies != self.origin_cookie:
                            raise LBMismatchError
                        if r['data']:
                            gevent.sleep(FORWARD_WORKER_SLEEP)
                    logger.warn('{} got empty listing. Sleep'.format(worker))
                    gevent.sleep(ON_EMPTY_DELAY)

                except LBMismatchError:
                    logger.info('LB mismatch error on backward worker')
                    self.reinit_clients.set()

        except Exception as e:
            logger.error("{} down! Error: {}".format(worker, e))
            self.forward_worker_dead.set()
        else:
            logger.error("{} finished.".format(worker))

    def _backward_worker(self, params):
        worker = "Backward worker: "
        logger.info('{} staring'.format(worker))
        try:
            while True:
                try:
                    r = self.backward_client.get_tenders(params)
                    if not r['data']:
                        logger.debug('{} empty listing..exiting'.format(worker))
                        break
                    gevent.sleep(BACKWARD_WOKER_DELAY)
                    if self.backward_client.session.cookies != self.origin_cookie:
                        raise LBMismatchError
                    try:
                        self.tender_queue.put(
                            filter(self.filter_callback, r['data'])
                        )
                    except Full:
                        logger.error('full queue')
                        while self.tender_queue.full():
                            gevent.sleep(QUEUE_FULL_DELAY)
                            self.tender_queue.put(
                                filter(self.filter_callback, r['data'])
                            )

                    params['offset'] = r['next_page']['offset']
                except LBMismatchError:
                    logger.info('{} LB mismatch error'.format(worker))
                    if not self.reinit_clients.is_set():
                        self.reinit_clients.set()

        except Exception as e:
            logger.error("{} down! Error: {}".format(worker, e))
            self.forward_worker_dead.set()
        else:
            logger.error("{} finished.".format(worker))

    def _restart_workers(self):
        self._init_clients()
        gevent.killall(self.workers)
        self._start_sync_workers()
        return self.workers

    def get_tenders(self):
        self._start_sync_workers()
        forward, backward = self.workers
        try:
            while True:
                if self.tender_queue.empty():
                    gevent.sleep(EMPTY_QUEUE_DELAY)
                if (forward.dead or forward.ready()) or \
                        (backward.dead and not backward.successful()):
                    forward, backward = self._restart_workers()
                while not self.tender_queue.empty():
                    yield self.tender_queue.get()
        except Exception as e:
            logger.error(e)
