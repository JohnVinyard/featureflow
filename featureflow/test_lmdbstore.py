import unittest2
from lmdbstore import LmdbDatabase
from uuid import uuid4
from data import StringDelimitedKeyBuilder
import shutil
import os
from multiprocessing.pool import Pool


class EphemeralLmdb(object):
    def __init__(self, dir=None):
        super(EphemeralLmdb, self).__init__()
        self.dir = dir or uuid4().hex
        self.path = '/tmp/{dir}'.format(dir=self.dir)
        self.key_builder = StringDelimitedKeyBuilder()
        self.init_database()
        self.key = self.key_builder.build('id', 'feature', 'version')

    def clean_up(self):
        shutil.rmtree(self.path, ignore_errors=True)

    def build_database(self):
        return LmdbDatabase(
            self.path,
            key_builder=self.key_builder)

    def init_database(self):
        self.db = self.build_database()

    def copy(self):
        return EphemeralLmdb(dir=self.dir)


def write_key(d):
    db = EphemeralLmdb(dir=d).db
    key_builder = StringDelimitedKeyBuilder()
    key = key_builder.build(uuid4().hex, 'feature', 'version')
    value = os.urandom(100)
    with db.write_stream(key, 'application/octet-stream') as ws:
        ws.write(value)
    return key, value


def db_count(d):
    return len(list(EphemeralLmdb(dir=d).db.iter_ids()))


class LmdbDatabaseTests(unittest2.TestCase):
    def setUp(self):
        self.value = os.urandom(1000)
        self.init_database()

    def tearDown(self):
        self.ephemeral.clean_up()

    def init_database(self, dir=None):
        self.ephemeral = EphemeralLmdb(dir=dir)
        self.dir = self.ephemeral.dir
        self.db = self.ephemeral.db
        self.key = self.ephemeral.key
        self.key_builder = self.ephemeral.key_builder

    def write_key(self):
        with self.db.write_stream(self.key, 'application/octet-stream') as ws:
            ws.write(self.value)

    def test_can_instantiate_db_many_times_without_causing_max_readers_error(
            self):
        for i in xrange(1000):
            db = EphemeralLmdb(dir=self.dir).db

            key = self.key_builder.build(uuid4().hex, 'feature', 'version')
            value = os.urandom(1000)

            with db.write_stream(key, 'application/octet-stream') as ws:
                ws.write(value)

            with db.read_stream(key) as rs:
                v = rs.read()

            db.close()

    def test_keys_written_out_of_process_are_reflected_in_current_process(self):
        pool = Pool(2)
        pool.map(write_key, [self.dir for _ in xrange(10)])
        _ids = list(self.db.iter_ids())
        self.assertEqual(10, len(_ids))

    def test_can_list_keys_from_multiple_processes(self):
        pool = Pool(4)
        for i in xrange(10):
            write_key(self.dir)

        counts = pool.map(db_count, [self.dir for _ in xrange(10)])

        self.assertEqual([10] * 10, counts)

    def test_key_error_is_raised_when_key_not_found(self):
        def x():
            with self.db.read_stream(self.key) as rs:
                value = rs.read()

        self.assertRaises(KeyError, x)

    def test_can_read_from_another_instance(self):
        # first, open another instance
        other_instance = self.ephemeral.copy().db
        # next write a key to the original instance
        self.write_key()
        # finally, read a key from the first-opened instance
        with other_instance.read_stream(self.key) as rs:
            value = rs.read()
            self.assertEqual(1000, len(value))

    def test_can_iter_ids_immediately_after_opening(self):
        self.write_key()
        self.assertEqual(1, len(list(self.db.iter_ids())))
        with self.db.read_stream(self.key) as rs:
            value = rs.read()
            self.assertEqual(1000, len(value))
        self.db.env.close()
        self.init_database(dir=self.dir)
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
            self.assertRaises(ValueError, lambda: rs.seek(0, 999))

    def test_can_iterate_over_empty_database(self):
        _ids = list(self.db.iter_ids())
        self.assertEqual(0, len(_ids))

    def test_does_not_create_key_when_no_bytes_written(self):
        key = self.key_builder.build(uuid4().hex, 'feature', 'version')
        with self.db.write_stream(key, 'application/octet-stream'):
            pass
        self.assertFalse(key in self.db)

    def test_does_not_create_key_when_zero_bytes_written(self):
        key = self.key_builder.build(uuid4().hex, 'feature', 'version')
        with self.db.write_stream(key, 'application/octet-stream') as ws:
            ws.write('')
        self.assertFalse(key in self.db)
