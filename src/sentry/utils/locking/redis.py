from uuid import uuid4

from sentry.utils.locking.manager import LockManager
from sentry.utils.redis import load_script


delete_lock = load_script('utils/locking/delete_lock.lua')


class RedisLockManager(LockManager):
    def __init__(self, cluster, uuid=None):
        if uuid is None:
            uuid = uuid4().hex

        self.cluster = cluster
        self.uuid = uuid

    def acquire(self, key, duration):
        client = self.cluster.get_local_client_for_key(key)
        if client.set(key, self.uuid, ex=duration, nx=True) is not True:
            raise Exception('Could not acquire lock!')

    def release(self, key):
        client = self.cluster.get_local_client_for_key(key)
        delete_lock(client, (key,), (self.uuid,))
