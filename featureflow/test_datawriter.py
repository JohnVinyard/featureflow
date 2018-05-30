import unittest2
from datawriter import StringIODataWriter
from encoder import IdentityEncoder


class StringIODataWriterTests(unittest2.TestCase):
    def test_overflow(self):
        buffer_size_limit = 128
        writer = StringIODataWriter(
            needs=IdentityEncoder(), buffer_size_limit=buffer_size_limit)
        data = 'a' * buffer_size_limit
        list(writer._process(data))
        writer._stream.seek(0)
        retrieved = writer._stream.read()
        self.assertEqual(data, retrieved)
