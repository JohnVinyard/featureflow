import lmdb
import binascii
import os
import time
import redis
import json
from Queue import Queue, Empty


class InMemoryChannel(object):
    def __init__(self):
        super(InMemoryChannel, self).__init__()
        self.generators = list()

    def subscribe(self, raise_when_empty=False):
        d = Queue()
        self.generators.append(d)

        def gen():
            while True:
                try:
                    data = json.loads(d.get(block=not raise_when_empty))
                    yield data['_id'], data['message']
                except Empty:
                    raise StopIteration

        return gen()

    def unsubscribe(self):
        raise NotImplementedError()

    def publish(self, _id, message):
        message = json.dumps({'_id': _id, 'message': message})
        for generator in self.generators:
            generator.put_nowait(message)


class RedisChannel(object):
    def __init__(self, channel, host='localhost', port=6379):
        super(RedisChannel, self).__init__()
        self.channel = channel
        self.r = redis.StrictRedis(host=host, port=port)
        self.p = self.r.pubsub(ignore_subscribe_messages=True)

    def unsubscribe(self):
        self.p.unsubscribe()

    def subscribe(self, raise_when_empty=False):
        if raise_when_empty:
            raise NotImplementedError(
                'raise_when_empty=True is not supported for RedisChannel')

        self.p.subscribe(self.channel)
        for message in self.p.listen():
            data = json.loads(message['data'])
            yield data['_id'], data['message']

    def publish(self, _id, message):
        self.r.publish(
            self.channel, json.dumps({'_id': _id, 'message': message}))


class EventLog(object):
    def __init__(self, path, channel, map_size=1000000000):
        super(EventLog, self).__init__()
        self.channel = channel
        self.path = path
        self.env = lmdb.open(
            self.path,
            max_dbs=10,
            map_size=map_size,
            writemap=True,
            map_async=True,
            metasync=True)

    def __len__(self):
        with self.env.begin() as txn:
            return txn.stat()['entries']

    def append(self, data):
        with self.env.begin(write=True) as txn:
            _id = hex(int(time.time() * 1e6)) + binascii.hexlify(os.urandom(8))
            txn.put(_id, data)
        self.channel.publish(_id, data)
        return _id

    def unsubscribe(self):
        self.channel.unsubscribe()

    def subscribe(self, last_id='', raise_when_empty=False):
        subscription = self.channel.subscribe(raise_when_empty=raise_when_empty)

        with self.env.begin() as txn:
            with txn.cursor() as cursor:
                if cursor.set_range(last_id):
                    for _id, data in cursor:
                        if _id == last_id:
                            continue
                        yield _id, data

        for _id, data in subscription:
            yield _id, data


