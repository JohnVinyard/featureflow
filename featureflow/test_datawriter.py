import unittest
from .datawriter import BytesIODataWriter
from .encoder import IdentityEncoder


class StringIODataWriterTests(unittest.TestCase):
    def test_overflow(self):
        buffer_size_limit = 128
        writer = BytesIODataWriter(
            needs=IdentityEncoder(), buffer_size_limit=buffer_size_limit)
        data = b'a' * buffer_size_limit
        list(writer._process(data))
        writer._stream.seek(0)
        retrieved = writer._stream.read()
        self.assertEqual(data, retrieved)
