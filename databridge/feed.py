import gevent
import logging
import sys
from gevent.queue import Queue
from .contrib.client import APICLient
from .contrib.retreive import RetreiverForward, RetreiverBackward
from .contrib.supervisor import RetreiversSupervisor


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

        if 'session' in options:
            self.session = options.pop('session')

        self._init_clients()

    def _init_clients(self):
        logger.info('Sync: Init clients')
        self.forward_client = APICLient(
            self.api_key,
            self.api_host,
            self.api_version,
            session=self.session or None
        )
        self.backward_client = APICLient(
            self.api_key,
            self.api_host,
            self.api_version,
            session=self.session or None
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

    def _start(self):
        logger.info('{} starting'.format(self.__class__))
        forward, backward = self._get_sync_point()
        self.forward_worker = RetreiverForward(
            self.forward_client,
            forward,
            self.origin_cookie,
            self.tender_queue,
            self.filter_callback,
            logger
        )
        self.backward_worker = RetreiverBackward(
            self.backward_client,
            backward,
            self.origin_cookie,
            self.tender_queue,
            self.filter_callback,
            logger
        )
        self.supervisor = RetreiversSupervisor(
            self.forward_worker,
            self.backward_worker,
            logger,
            [Exception],
            delay=3
        )
        self.supervisor.link_exception(self._restart)
        self.supervisor.start()

    def _restart(self, worker):
        if worker is not None or not worker.ready():
            worker.kill()
        worker.start()

    def __iter__(self):
        try:
            self._start()
        except Exception as e:
            logger.error('Error during start {}'.format(e))
            sys.exit(2)
        try:
            while True:
                if self.tender_queue.empty():
                    gevent.sleep(EMPTY_QUEUE_DELAY)
                try:
                    res = self.supervisor.get(block=False)
                except gevent.Timeout:
                    pass
                else:
                    if res == 0:
                        self._restart(self.supervisor)

                while not self.tender_queue.empty():
                    yield self.tender_queue.get()
        except Exception as e:
            logger.error(e)
