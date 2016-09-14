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

    def add_workers(self, workers, config=None):
        src = self.src_queue
        for worker in workers:
            setattr(self, "{}_queue".format(worker.__name__), Queue(maxsize=250))
            self.workers[worker.__name__] = worker(
                src,
                getattr(self, "{}_queue".format(worker.__name__)),
                config
            )
            src = getattr(self, "{}_queue".format(worker.__name__))
            self.dest_queue = getattr(self, "{}_queue".format(worker.__name__))

    def _start_workers(self):
        for attr, worker in self.workers.items():
            worker.start()

    def _restart_workers(self):
        for worker in self.workers.values():
            worker.kill()

        for worker in self.workers.values():
            worker.start()

    def run(self):
        logger.debug('{}: starting'.format(self.__class__))
        self._start_workers()
        while True:
            while not self.dest_queue.empty():
                for attr, worker in self.workers.items():
                    if worker.dead or worker.ready():
                        if worker.exception:
                            logger.error('Worker: {} error {}'.format(
                                worker.__class__, worker.expection))
                        else:
                            logger.warn('Worker {} not active.. restaring'.format(worker.__class__))
                    self._restart_workers()
                gevent.sleep(3)
