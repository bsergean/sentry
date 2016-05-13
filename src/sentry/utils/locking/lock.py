import functools
import logging
from contextlib import contextmanager


logger = logging.getLogger(__name__)


class Lock(object):
    def __init__(self, manager, key, duration):
        self.manager = manager
        self.key = key
        self.duration = duration

    def __repr__(self):
        return '<Lock: {}>'.format(self.key)

    def __call__(self):
        # NOTE: We acquire the lock immediately and *return* a new context
        # manager so that this ``__call__`` method can be wrapped in a retry
        # policy.
        self.acquire()

        @contextmanager
        def releaser():
            try:
                yield
            finally:
                self.release()

        return releaser()

    def acquire(self):
        self.manager.acquire(self.key, self.duration)

    def release(self):
        try:
            self.manager.release(self.key)
        except Exception as error:
            logger.warning('Failed to release lock (%r) due to error: %s', self, error, exc_info=True)
