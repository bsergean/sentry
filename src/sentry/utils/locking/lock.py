import functools
import logging
from contextlib import contextmanager

from sentry.utils.locking.retries import NoRetryPolicy


logger = logging.getLogger(__name__)


class Lock(object):
    def __init__(self, manager, key, duration):
        self.manager = manager
        self.key = key
        self.duration = duration

    def __repr__(self):
        return '<Lock: {}>'.format(self.key)

    @contextmanager
    def __call__(self, *args, **kwargs):
        self.acquire(*args, **kwargs)
        try:
            yield
        finally:
            self.release()

    def acquire(self, retry=NoRetryPolicy()):
        return retry(
            functools.partial(self.manager.acquire, self.key, self.duration),
        )

    def release(self):
        try:
            self.manager.release(self.key)
        except Exception as error:
            logger.warning('Failed to release lock (%r) due to error: %s', self, error, exc_info=True)
