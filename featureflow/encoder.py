import json
from extractor import Node, Aggregator
import bz2
from cPickle import dumps


class IdentityEncoder(Node):
    content_type = 'application/octet-stream'

    def __init__(self, needs=None):
        super(IdentityEncoder, self).__init__(needs=needs)

    def _enqueue(self, data, pusher):
        self._cache = data if data else ''


class TextEncoder(IdentityEncoder):
    content_type = 'text/plain'

    def __init__(self, needs=None):
        super(TextEncoder, self).__init__(needs=needs)


class JSONEncoder(Aggregator, Node):
    content_type = 'application/json'

    def __init__(self, needs=None):
        super(JSONEncoder, self).__init__(needs=needs)

    def _process(self, data):
        yield json.dumps(data)


class PickleEncoder(Aggregator, Node):
    content_type = 'application/octet-stream'

    def __init__(self, needs=None):
        super(PickleEncoder, self).__init__(needs=needs)

    def _process(self, data):
        yield dumps(data)


class BZ2Encoder(Node):
    content_type = 'application/octet-stream'

    def __init__(self, needs=None):
        super(BZ2Encoder, self).__init__(needs=needs)
        self._compressor = None

    def _finalize(self, pusher):
        self._cache = ''

    def _first_chunk(self, data):
        self._compressor = bz2.BZ2Compressor()
        return data

    def _last_chunk(self):
        yield self._compressor.flush()

    def _process(self, data):
        compressed = self._compressor.compress(data)
        if compressed:
            yield compressed
