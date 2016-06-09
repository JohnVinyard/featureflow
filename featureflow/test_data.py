import unittest2
from uuid import uuid4
from data import \
    InMemoryDatabase, UserSpecifiedIdProvider, FileSystemDatabase, \
    StringDelimitedKeyBuilder
import shutil


class InMemoryDatabaseTest(unittest2.TestCase):
    def setUp(self):
        self.db = InMemoryDatabase()

    def set(self, k, v):
        flo = self.db.write_stream(k, 'text/plain')
        flo.write(v)
        flo.close()

    def get(self, k):
        return self.db.read_stream(k)

    def test_can_write_data(self):
        self.set('key', 'test data')

    def test_can_read_data(self):
        self.set('key', 'test data')
        rs = self.get('key')
        self.assertEqual('test data', rs.read())

    def test_can_overwrite_key(self):
        self.set('key', 'test data')
        rs = self.get('key')
        self.assertEqual('test data', rs.read())
        self.set('key', 'test data2')
        rs = self.get('key')
        self.assertEqual('test data2', rs.read())


class UserSpecifiedIdProviderTest(unittest2.TestCase):
    def test_raises_when_no_key_is_provided(self):
        self.assertRaises(ValueError, lambda: UserSpecifiedIdProvider())


class FileSystemDatabaseTests(unittest2.TestCase):
    def setUp(self):
        self._key_builder = StringDelimitedKeyBuilder()
        self._path = '/tmp/{path}'.format(path=uuid4().hex)

    def tearDown(self):
        try:
            shutil.rmtree(self._path)
        except OSError:
            pass

    def _make_db(self, createdirs):
        return FileSystemDatabase(
                path=self._path,
                key_builder=self._key_builder,
                createdirs=createdirs)

    def test_creates_path_when_asked(self):
        db = self._make_db(createdirs=True)
        with db.write_stream('key', 'text/plain') as s:
            s.write('text')

        with db.read_stream('key') as s:
            self.assertEqual('text', s.read())

    def test_does_not_create_path_when_not_asked(self):
        db = self._make_db(createdirs=False)
        self.assertRaises(IOError, lambda: db.write_stream('key', 'text/plain'))
