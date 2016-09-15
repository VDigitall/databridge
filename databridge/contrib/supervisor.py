import gevent


class RetreiversSupervisor(gevent.Greenlet):

    def __init__(self, forward, backward,  logger, allowed_errors, delay=2):
        super(RetreiversSupervisor, self).__init__()
        self.forward = forward
        self.backward = backward
        for g in [self.forward, self.backward]:
            g.link_exception(self._handle_retreiver_error)
        self.logger = logger
        self.allowed_errors = allowed_errors
        self.watch_delay = delay

    def _handle_retreiver_error(self, worker):
        self.logger.warn('Exception {} in {}'.format(
            worker.exception, worker.__class__))
        if worker.exception not in self.allowed_errors:
            self.logger.info('Unhandled exception {}'.format(worker.error))
            raise gevent.GreenletExit
        if worker == self.forward:
            self._restart_retreivers()
        else:
            if self.backward is not None or self.backward.ready():
                self.logger.info('{} is still active'.format(worker.__class__))
                self.backward.kill()
            self.logger.info('{}: starting'.format(worker.__class__))
            self.backward.start()

    def _restart_workers(self):
        for g in [self.forward, self.backward]:
            if g is not None or not g.ready():
                g.kill(timeout=1)
            g.start()

    def _run(self):
        self.logger.info("{} start watching {} {}".format(
            self.__class__, self.forward.__class__, self.backward.__class__))
        for g in [self.forward, self.backward]:
            g.start()
        while True:
            for g in [self.forward, self.backward]:
                try:
                    res = g.get(block=False)
                except gevent.Timeout:
                    self.logger.info('{} is still active'.format(g.__class__))
                    gevent.sleep(self.watch_delay)
                    continue
                except Exception as e:
                    self.logger.error('{} fails with {}'.format(g.__class__, e))
                    if g is not None or not g.ready():
                        g.kill(timeout=1)
                    g.start()
                if res and g == self.forward:
                    self.logger.ward('Forward worker died. Restarting')
                    self._restart_workers()
                else:
                    if res == 0:
                        self.logger.info('Backward finished')
                        g.start()
                gevent.sleep(self.watch_delay)
        return 0


class DataBridgeSupervisor(gevent.Greenlet):

    def __init__(self, workers, logger, delay=2):
        super(DataBridgeSupervisor, self).__init__()
        self.workers = workers
        self.logger = logger
        self.delay = delay

        [g.link_exception(self._restart_worker) for g in self.workers.values()]

    def _restart_worker(self, worker):
        if worker.exception:
            self.logger.error('Exception {} in {}'.format(
                worker.exception, worker.__class__))
        if worker is not None or not worker.ready():
            worker.kill()
        self.logger.info('Starting {}'.format(worker.__class__))
        worker.start()

    def _run(self):
        self.logger.info('Start supervising {}'.format([
            g for g in self.workers.keys()
        ]))
        for name, g in self.workers.items():
            self.logger.info('Starting {}'.format(name))
            g.start()

        while True:
            for name, g in self.workers.items():
                try:
                    res = g.get(block=False)
                except gevent.Timeout:
                    gevent.sleep(self.delay)
                    continue
                except Exception as e:
                    self.logger.error('{} fails with {}'.format(name, e))
                    if g is not None or not g.ready():
                        g.kill(timeout=1)
                    g.start()
                    gevent.sleep(self.delay)
                    continue
                if res and res == 1:
                    self.logger.warn('{} finished work'.format(name))
                    g.start()
                else:
                    self.logger.error('{} returns not 1 value'.format(name))
                    raise gevent.GreenletExit
                gevent.sleep(self.delay)
        return 0
