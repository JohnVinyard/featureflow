import unittest
from .objectstore import WriteStream
import http.client
from collections import namedtuple


class WriteStreamTests(unittest.TestCase):
    def test_write_stream_does_not_put_zero_bytes(self):
        class TestWriteStream(WriteStream):
            def __init__(self):
                super(TestWriteStream, self).__init__(None, None, None, None)
                self._put_count = 0

            def _put(self):
                self._put_count += 1
                response_cls = namedtuple('Response', 'status_code')
                return response_cls(status_code=http.client.NO_CONTENT)

        ws = TestWriteStream()
        ws.close()
        self.assertEqual(0, ws._put_count)
