from extractor import Node
from decoder import Decoder
from feature import Feature
from util import chunked
import requests
from urlparse import urlparse
import os
import struct


class ByteStream(Node):
    def __init__(self, chunksize=4096, needs=None):
        super(ByteStream, self).__init__(needs=needs)
        self._chunksize = chunksize

    def _generator(self, stream, content_length):
        if not content_length:
            raise ValueError('content_length should be greater than zero')
        for chunk in chunked(stream, chunksize=self._chunksize):
            yield StringWithTotalLength(chunk, content_length)

    def _from_http_response(self, resp):
        resp.raise_for_status()
        content_length = int(resp.headers['Content-Length'])
        return self._generator(resp.raw, content_length)

    def _handle_simple_get(self, data):
        parsed = urlparse(data)
        if parsed.scheme and parsed.netloc:
            resp = requests.get(data, stream=True)
            return self._from_http_response(resp)
        else:
            raise ValueError

    def _handle_http_request(self, data):
        s = requests.Session()
        prepped = data.prepare()
        resp = s.send(prepped, stream=True)
        return self._from_http_response(resp)

    def _handle_file_like_object(self, data):
        content_length = data.seek(0, 2)
        data.seek(0)
        return self._generator(data, content_length)

    def _handle_file(self, data):
        with open(data, 'rb') as f:
            content_length = int(os.path.getsize(data))
            for chunk in self._generator(f, content_length):
                yield chunk

    def _get_strategy(self, data):
        if isinstance(data, requests.Request):
            return self._handle_http_request
        if isinstance(data, str):
            parsed = urlparse(data)
            is_url = parsed.netloc and parsed.scheme
            return self._handle_simple_get if is_url else self._handle_file
        return self._handle_file_like_object

    def _process(self, data):
        try:
            data = data.uri
        except AttributeError:
            pass
        strategy = self._get_strategy(data)
        for chunk in strategy(data):
            yield chunk


class StringWithTotalLength(str):
    def __new__(cls, s, total_length):
        o = str.__new__(cls, s)
        o.total_length = int(total_length)
        return o

    def __radd__(self, other):
        return StringWithTotalLength(other + str(self), self.total_length)


class StringWithTotalLengthEncoder(Node):
    content_type = 'application/octet-stream'

    def __init__(self, needs=None):
        super(StringWithTotalLengthEncoder, self).__init__(needs=needs)
        self._metadata_written = False

    def _process(self, data):
        if not self._metadata_written:
            yield struct.pack('I', data.total_length)
            self._metadata_written = True
        yield data


class StringWithTotalLengthDecoder(Decoder):
    def __init__(self, chunksize=4096):
        super(StringWithTotalLengthDecoder, self).__init__()
        self._chunksize = chunksize
        self._total_length = None

    def __call__(self, flo):
        return self.__iter__(flo)

    def __iter__(self, flo):
        self._total_length = struct.unpack('I', flo.read(4))[0]
        for chunk in chunked(flo, self._chunksize):
            yield StringWithTotalLength(chunk, self._total_length)


class ByteStreamFeature(Feature):
    def __init__(
            self,
            extractor,
            needs=None,
            store=False,
            key=None,
            **extractor_args):
        super(ByteStreamFeature, self).__init__(
                extractor,
                needs=needs,
                store=store,
                encoder=StringWithTotalLengthEncoder,
                decoder=StringWithTotalLengthDecoder(
                        chunksize=extractor_args['chunksize']),
                key=key,
                **extractor_args)
