import gevent
import logging
import sys
from gevent.queue import Queue
from .feed import APIRetreiver
from .contrib.supervisor import DataBridgeSupervisor


logger = logging.getLogger(__name__)


class APIDataBridge(object):

    def __init__(self, config, filter_feed=lambda x: x, session=None):
        if not isinstance(config, dict):
            raise TypeError(
                "Expected a dict as config, got {}".format(type(config))
            )
        self.retreiver = APIRetreiver(
            config, filter_callback=filter_feed, session=session)
        self.workers = {}

    def add_workers(self, workers, config=None):
        src = self.retreiver
        for worker in workers:
            logger.debug('Add worker {}'.format(worker.__name__))
            setattr(self, "{}_queue".format(worker.__name__),
                    Queue(maxsize=250))
            self.workers[worker.__name__] = worker(
                src,
                getattr(self, "{}_queue".format(worker.__name__)),
                config
            )
            src = getattr(self, "{}_queue".format(worker.__name__))
            self.dest_queue = src
        self.supervisor = DataBridgeSupervisor(self.workers, logger)
        self.supervisor.link_exception(self._restart)

    def _restart(self, worker):
        if worker is not None or not worker.ready():
            worker.kill()
        worker.start()

    def run(self):
        logger.debug('{}: starting'.format(self.__class__))
        try:
            self.supervisor.start()
        except Exception as e:
            logger.error('Error during start {}'.format(e))
            sys.exit(2)
        while True:
            while not self.dest_queue.empty():
                logger.debug(self.dest_queue.get())
            gevent.sleep(3)
