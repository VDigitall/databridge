import gevent
import logging
from gevent.queue import Queue
from .feed import APIRetreiver


logger = logging.getLogger(__name__)


class APIDataBridge(object):

    def __init__(self, config, filter_feed=lambda x: x):
        if not isinstance(config, dict):
            raise TypeError(
                "Expected a dict as config, got {}".format(type(config))
            )
        self.retreiver = APIRetreiver(config, filter_callback=filter_feed)
        self.workers = {}
        self.src_queue = self.retreiver.get_tenders

    def add_worker(self, worker, config=None):
        setattr(self, "{}_queue".format(worker.__name__), Queue(maxsize=250))
        self.workers[worker.__name__] = worker(
            self.src_queue,
            getattr(self, "{}_queue".format(worker.__name__)),
            config
        )
        self.src_queue = getattr(self, "{}_queue".format(worker.__name__))

    def _start_workers(self):
        for attr, worker in self.workers.items():
            worker.start()

    def run(self):
        logger.debug('{}: starting'.format(self.__class__))
        self._start_workers()
        while True:
            while not self.src_queue.empty():
                print self.src_queue.get()
                for attr, worker in self.workers.items():
                    if worker.dead or worker.ready():
                        if worker.expection:
                            logger.error('Worker: {} error {}'.format(
                                worker.__class, worker.expection))
                        else:
                            logger.warn(
                                'Worker {} not active.. restaring'.format(worker.__class__))
            gevent.sleep(3)
