import json
from util import chunked
from extractor import Node
import bz2
from cPickle import loads


class Decoder(object):
    """
    The simplest possible decoder takes a file-like object and returns it
    """

    def __init__(self):
        super(Decoder, self).__init__()

    def __call__(self, flo):
        return flo

    def __iter__(self, flo):
        for chunk in chunked(flo):
            yield chunk


class GreedyDecoder(Decoder):
    """
    A decoder that reads the entire file contents into memory
    """

    def __init__(self):
        super(GreedyDecoder, self).__init__()

    def __call__(self, flo):
        return flo.read()

    def __iter__(self, flo):
        yield self(flo)


class JSONDecoder(GreedyDecoder):
    """
    A decoder that interprets the data as JSON
    """

    def __init__(self):
        super(JSONDecoder, self).__init__()

    def __call__(self, flo):
        s = super(JSONDecoder, self).__call__(flo)
        return json.loads(s)

    def __iter__(self, flo):
        yield self(flo)


class PickleDecoder(GreedyDecoder):
    def __init__(self):
        super(PickleDecoder, self).__init__()

    def __call__(self, flo):
        return loads(flo.read())

    def __iter__(self, flo):
        yield self(flo)


class BZ2Decoder(Decoder):
    """
    A decoder that decompresses data using the bz2 compression algorithm
    """

    def __init__(self):
        super(BZ2Decoder, self).__init__()

    def __call__(self, flo):
        return self.__iter__(flo)

    def __iter__(self, flo):
        decompressor = bz2.BZ2Decompressor()
        for chunk in chunked(flo):
            yield decompressor.decompress(chunk)


class DecoderNode(Node):
    def __init__(self, needs=None, decodifier=None, version=None):
        super(DecoderNode, self).__init__(needs=needs)
        self._version = version
        self.decoder = decodifier

    @property
    def version(self):
        return self._version

    def _process(self, data):
        for x in self.decoder.__iter__(data):
            yield x
