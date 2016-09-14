import gevent
import logging
from gevent.queue import Queue
from .feed import APIRetreiver


logger = logging.getLogger(__name__)


class APIDataBridge(object):

    def __init__(self, config, filter_feed=lambda x: x, session=None):
        if not isinstance(config, dict):
            raise TypeError(
                "Expected a dict as config, got {}".format(type(config))
            )
        logger.error(session)
        self.retreiver = APIRetreiver(
            config, filter_callback=filter_feed, session=session)
        self.workers = {}

    def add_workers(self, workers, config=None):
        src = self.retreiver
        for worker in workers:
            logger.debug('adapting worker {}'.format(worker.__name__))
            setattr(self, "{}_queue".format(worker.__name__), 
                    Queue(maxsize=250))
            self.workers[worker.__name__] = worker(
                src,
                getattr(self, "{}_queue".format(worker.__name__)),
                config
            )
            src = getattr(self, "{}_queue".format(worker.__name__))
            self.dest_queue = src

    def _start_workers(self):
        for attr, worker in self.workers.items():
            worker.start()

    def _restart_workers(self):
        for worker in self.workers.values():
            worker.start()

    def run(self):
        logger.debug('{}: starting'.format(self.__class__))
        self._start_workers()
        while True:
            while not self.dest_queue.empty():
                logger.debug(self.dest_queue.get())
                for attr, worker in self.workers.items():
                    if worker.dead or worker.ready():
                        if worker.exception:
                            logger.error('Worker: {} error {}'.format(
                                worker.__class__, worker.expection))
                        else:
                            logger.warn('Worker {} not active.. restaring'.format(worker.__class__))
                    self._restart_workers()
            gevent.sleep(3)
