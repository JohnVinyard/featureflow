from bytestream import StringWithTotalLength, ByteStream
import unittest2
import sys
import tempfile
import subprocess
import requests
import time
from io import BytesIO
from collections import namedtuple
import os
from uuid import uuid4


class BytestreamTests(unittest2.TestCase):
    def setUp(self):
        self.HasUri = namedtuple('HasUri', ['uri'])
        self.bytestream = ByteStream(chunksize=3)
        self.port = '9876'
        path = os.path.dirname(__file__)
        server = os.path.join(path, 'dummyserver.py')
        self.expected = ''.join(uuid4().hex for _ in xrange(100))
        devnull = open(os.devnull, 'w')
        self.process = subprocess.Popen(
                [sys.executable, server, self.port, self.expected],
                stdout=devnull,
                stderr=devnull)
        time.sleep(0.1)

    def tearDown(self):
        self.process.kill()

    def results(self, inp):
        return ''.join(self.bytestream._process(inp))

    def local_url(self):
        return 'http://localhost:{port}'.format(**self.__dict__)

    def get_request(self):
        return requests.Request(
                method='GET',
                url=self.local_url())

    def test_throws_on_zero_length_stream(self):
        with tempfile.NamedTemporaryFile('w+') as tf:
            tf.write('')
            tf.seek(0)
            self.assertRaises(ValueError, lambda: self.results(tf.name))

    def test_can_use_local_file(self):
        with tempfile.NamedTemporaryFile('w+') as tf:
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
        with tempfile.NamedTemporaryFile('w+') as tf:
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


class StringWithTotalLengthTests(unittest2.TestCase):
    def test_left_add(self):
        self.assertEqual(
                'fakeblah', StringWithTotalLength('fake', 100) + 'blah')

    def test_right_add(self):
        self.assertEqual(
                'blahfake', 'blah' + StringWithTotalLength('fake', 100))

    def test_left_increment(self):
        x = StringWithTotalLength('fake', 100)
        x += 'blah'
        self.assertEqual('fakeblah', x)

    def test_right_increment(self):
        x = 'blah'
        x += StringWithTotalLength('fake', 100)
        self.assertEqual('blahfake', x)
