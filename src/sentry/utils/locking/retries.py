import logging
import time


__all__ = (
    'BackoffRetryPolicy',
    'NoRetryPolicy',
)


logger = logging.getLogger(__name__)


class RetryPolicy(object):
    def __call__(self, function):
        raise NotImplementedError


class NoRetryPolicy(RetryPolicy):
    def __call__(self, function):
        return function()


class BackoffRetryPolicy(RetryPolicy):
    def __init__(self, attempts, delay=lambda i: i, exceptions=(Exception,)):
        assert attempts > 0
        self.attempts = attempts
        self.delay = delay
        self.exceptions = exceptions

    def __call__(self, function):
        for i in xrange(1, self.attempts + 1):
            try:
                return function()
            except self.exceptions as error:
                if i < self.attempts:
                    delay = self.delay(i)
                    logger.warning(
                        'Failed to execute %r due to %r on attempt #%s, retrying in %s seconds...',
                        function,
                        error,
                        i,
                        delay,
                    )
                    time.sleep(delay)
                else:
                    raise
