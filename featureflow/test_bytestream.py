from .bytestream import BytesWithTotalLength, ByteStream, ZipWrapper, iter_zip
import unittest2
import sys
import tempfile
import subprocess
import requests
from io import BytesIO
from collections import namedtuple
import os
from uuid import uuid4
import zipfile
from .util import wait_for_http_server


class BytestreamTests(unittest2.TestCase):
    def setUp(self):
        self.HasUri = namedtuple('HasUri', ['uri'])
        self.bytestream = ByteStream(chunksize=3)
        self.port = '9876'
        path = os.path.dirname(__file__)
        server = os.path.join(path, 'dummyserver.py')
        self.expected = b''.join(uuid4().hex.encode() for _ in range(100))
        devnull = open(os.devnull, 'w')
        self.process = subprocess.Popen(
            [sys.executable, server, self.port, self.expected],
            stdout=devnull,
            stderr=devnull)
        wait_for_http_server('localhost', '9876')

    def tearDown(self):
        self.process.kill()

    def results(self, inp):
        return b''.join(self.bytestream._process(inp))

    def local_url(self):
        return 'http://localhost:{port}'.format(**self.__dict__)

    def get_request(self):
        return requests.Request(
            method='GET',
            url=self.local_url())

    def test_can_accept_chunksize_implementing_int(self):
        class SemanticChunksize(object):
            THING_SIZE_BYTES = 64

            def __init__(self, n_things=10):
                self.n_things = n_things

            def __int__(self):
                return self.n_things * self.THING_SIZE_BYTES

        bs = ByteStream(
            chunksize=SemanticChunksize(n_things=100))
        bio = BytesIO()
        bio.write(os.urandom(int(1e5)))
        bio.seek(0)
        chunks = list(bs._process(bio))
        self.assertEqual(6400, len(chunks[0]))

    def test_throws_on_zero_length_stream(self):
        with tempfile.NamedTemporaryFile('w+') as tf:
            tf.write('')
            tf.seek(0)
            self.assertRaises(ValueError, lambda: self.results(tf.name))

    def test_can_use_zip_file(self):
        bio = BytesIO()
        fn = 'test.dat'
        with zipfile.ZipFile(bio, mode='w') as zf:
            zf.writestr(fn, self.expected)
        bio.seek(0)

        with zipfile.ZipFile(bio) as zf:
            with zf.open(fn) as x:
                wrapper = ZipWrapper(x, zf.getinfo(fn))
                results = self.results(wrapper)
        self.assertEqual(self.expected, results)

    def test_can_use_local_file(self):
        with tempfile.NamedTemporaryFile('wb+') as tf:
            tf.write(self.expected)
            tf.seek(0)
            results = self.results(tf.name)
            self.assertEqual(self.expected, results)

    def test_can_use_file_like_object(self):
        bio = BytesIO(self.expected)
        results = self.results(bio)
        self.assertEqual(self.expected, results)

    def test_can_pass_url_as_string(self):
        url = self.local_url()
        results = self.results(url)
        self.assertEqual(self.expected, results)

    def test_can_pass_http_request(self):
        req = self.get_request()
        results = self.results(req)
        self.assertEqual(self.expected, results)

    def test_supports_legacy_uri_interface_for_files(self):
        with tempfile.NamedTemporaryFile('wb+') as tf:
            tf.write(self.expected)
            tf.seek(0)
            results = self.results(self.HasUri(uri=tf.name))
            self.assertEqual(self.expected, results)

    def test_supports_legacy_uri_interface_for_requests(self):
        req = self.get_request()
        results = self.results(self.HasUri(uri=req))
        self.assertEqual(self.expected, results)

    def test_supports_legacy_uri_interface_for_file_like_objects(self):
        bio = BytesIO(self.expected)
        results = self.results(self.HasUri(uri=bio))
        self.assertEqual(self.expected, results)


class BytesWithTotalLengthTests(unittest2.TestCase):
    def test_left_add(self):
        self.assertEqual(
            b'fakeblah', BytesWithTotalLength(b'fake', 100) + b'blah')

    def test_right_add(self):
        self.assertEqual(
            b'blahfake', b'blah' + BytesWithTotalLength(b'fake', 100))

    def test_left_increment(self):
        x = BytesWithTotalLength(b'fake', 100)
        x += b'blah'
        self.assertEqual(b'fakeblah', x)

    def test_right_increment(self):
        x = b'blah'
        x += BytesWithTotalLength(b'fake', 100)
        self.assertEqual(b'blahfake', x)


class IterZipTests(unittest2.TestCase):
    def test_iter_zip_yields_open_zip_files(self):
        bio = BytesIO()
        filename = 'test.dat'
        with zipfile.ZipFile(bio, mode='w') as zf:
            zf.writestr(filename, b'content')
        bio.seek(0)

        with list(iter_zip(bio))[0] as z:
            self.assertFalse(
                z.zipfile.closed, 'zipfile should be open, but was closed')
            self.assertEqual(z.zipfile.read(), b'content')
