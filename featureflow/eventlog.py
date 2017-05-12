import lmdb
import binascii
import os
import time
import redis
import json
from collections import deque


class InMemoryChannel(object):
    def __init__(self):
        super(InMemoryChannel, self).__init__()
        self.generators = list()

    def subscribe(self):
        d = deque()
        self.generators.append(d)

        def gen():
            while True:
                try:
                    data = json.loads(d.popleft())
                    yield data['_id'], data['message']
                except IndexError:
                    continue

        return gen()

    def publish(self, _id, message):
        message = json.dumps({'_id': _id, 'message': message})
        for generator in self.generators:
            generator.append(message)


class RedisChannel(object):
    def __init__(self, channel):
        super(RedisChannel, self).__init__()
        self.channel = channel
        self.r = redis.StrictRedis()
        self.p = self.r.pubsub(ignore_subscribe_messages=True)
        self.p.subscribe(channel)

    def subscribe(self):
        for message in self.p.listen():
            data = json.loads(message['data'])
            yield data['_id'], data['message']

    def publish(self, _id, message):
        self.r.publish(
            self.channel, json.dumps({'_id': _id, 'message': message}))


class EventStore(object):
    def __init__(self, path, channel, map_size=1000000000):
        super(EventStore, self).__init__()
        self.channel = channel
        self.path = path
        self.env = lmdb.open(
            self.path,
            max_dbs=10,
            map_size=map_size,
            writemap=True,
            map_async=True,
            metasync=True)

    def append(self, data):
        with self.env.begin(write=True) as txn:
            _id = hex(int(time.time() * 1e6)) + binascii.hexlify(os.urandom(8))
            txn.put(_id, data)
        self.channel.publish(_id, data)

    def subscribe(self, last_id):
        with self.env.begin(buffers=True) as txn:
            cursor = txn.cursor()
            if cursor.set_range(last_id):
                for _id, data in cursor:
                    yield _id, data

        for _id, data in self.channel.subscribe():
            yield _id, data


