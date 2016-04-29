import unittest2
from data import InMemoryDatabase, UserSpecifiedIdProvider


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
