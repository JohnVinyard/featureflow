import unittest2
from lmdbstore import LmdbDatabase
from uuid import uuid4
from data import StringDelimitedKeyBuilder
import shutil
import os


class LmdbDatabaseTests(unittest2.TestCase):
    def setUp(self):
        self.path = '/tmp/{dir}'.format(dir=uuid4().hex)
        self.key_builder = StringDelimitedKeyBuilder()
        self.init_database()
        self.value = os.urandom(1000)
        self.key = self.key_builder.build('id', 'feature', 'version')

    def tearDown(self):
        shutil.rmtree(self.path, ignore_errors=True)

    def init_database(self):
        self.db = LmdbDatabase(
                self.path,
                key_builder=self.key_builder)

    def write_key(self):
        with self.db.write_stream(self.key, 'application/octet-stream') as ws:
            ws.write(self.value)

    def test_can_iter_ids_immediately_after_opening(self):
        self.write_key()
        self.assertEqual(1, len(list(self.db.iter_ids())))
        with self.db.read_stream(self.key) as rs:
            value = rs.read()
            self.assertEqual(1000, len(value))
        self.db.env.close()
        self.init_database()
        self.assertEqual(1, len(list(self.db.iter_ids())))
        with self.db.read_stream(self.key) as rs:
            value = rs.read()
            self.assertEqual(1000, len(value))

    def test_can_seek_to_beginning_of_value(self):
        self.write_key()
        with self.db.read_stream(self.key) as rs:
            rs.read(100)
            rs.seek(0, os.SEEK_SET)
            self.assertEqual(0, rs.tell())
            self.assertEqual(rs.read(100), self.value[:100])

    def test_can_seek_relative_to_current_position(self):
        self.write_key()
        with self.db.read_stream(self.key) as rs:
            rs.read(100)
            rs.seek(100, os.SEEK_CUR)
            self.assertEqual(200, rs.tell())
            self.assertEqual(rs.read(100), self.value[200:300])

    def test_can_seek_relative_to_end_of_value(self):
        self.write_key()
        with self.db.read_stream(self.key) as rs:
            rs.seek(-100, os.SEEK_END)
            self.assertEqual(900, rs.tell())
            self.assertEqual(rs.read(100), self.value[-100:])

    def test_invalid_seek_argument_raises(self):
        self.write_key()
        with self.db.read_stream(self.key) as rs:
            self.assertRaises(IOError, lambda: rs.seek(0, 999))

    def test_can_iterate_over_empty_database(self):
        _ids = list(self.db.iter_ids())
        self.assertEqual(0, len(_ids))


