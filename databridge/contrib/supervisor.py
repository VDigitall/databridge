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
            if g is not None or g.ready():
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
                if res and g == self.forward:
                    self.logger.ward('Forward worker died. Restarting')
                    self._restart_workers()
                else:
                    if res == 0:
                        self.logger.info('Backward finished')
                        g.start()
                gevent.sleep(self.watch_delay)
        return 0
