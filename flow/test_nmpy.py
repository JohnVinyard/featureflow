import unittest2

try:
    import numpy as np
    from nmpy import NumpyFeature, StreamingNumpyDecoder
except ImportError:
    np = None

from persistence import PersistenceSettings
from data import *
from model import BaseModel
from util import TempDir
from lmdbstore import LmdbDatabase
from extractor import Node


class PassThrough(Node):
    def __init__(self, needs=None):
        super(PassThrough, self).__init__(needs=needs)

    def _process(self, data):
        yield data


class BaseNumpyTest(object):
    def setUp(self):
        if np is None:
            self.skipTest('numpy is not available')

        # Registry.register(IdProvider, UuidProvider())
        # Registry.register(KeyBuilder, StringDelimitedKeyBuilder())
        # self._register_database()
        # Registry.register(DataWriter, DataWriter)

        class Settings(PersistenceSettings):
            id_provider = UuidProvider()
            key_builder = StringDelimitedKeyBuilder()
            database = InMemoryDatabase(key_builder=key_builder)

        self.Settings = self._register_database(Settings)

    def _check_array(self, arr, shape, dtype, orig):
        self.assertTrue(isinstance(arr, np.ndarray))
        self.assertTrue(np.all(arr == orig))
        self.assertEqual(shape, arr.shape)
        self.assertEqual(dtype, arr.dtype)

    def _build_doc(self):
        class Doc(BaseModel, self.Settings):
            feat = NumpyFeature(PassThrough, store=True)

        return Doc

    def _restore(self, data):
        return data

    def _arrange(self, shape=None, dtype=None):
        cls = self._build_doc()
        arr = np.recarray(shape, dtype=dtype) \
            if isinstance(dtype, list) else np.zeros(shape, dtype=dtype)
        _id = cls.process(feat=arr)
        doc = cls(_id)
        recovered = self._restore(doc.feat)
        self._check_array(recovered, shape, dtype, arr)

    def _register_database(self):
        raise NotImplemented()

    def test_can_store_and_retrieve_empty_array(self):
        self._arrange((0,), np.uint8)

    def test_can_store_and_retrieve_1d_float32_array(self):
        self._arrange((33,), np.float32)

    def test_can_store_and_retreive_multidimensional_uint8_array(self):
        self._arrange((12, 13), np.uint8)

    def test_can_store_and_retrieve_multidimensional_float32_array(self):
        self._arrange((5, 10, 11), np.float32)

    def test_can_store_and_retrieve_recarray(self):
        self._arrange(shape=(25,), dtype=[ \
            ('x', np.uint8, (509,)),
            ('y', 'a32')])


class GreedyNumpyTest(BaseNumpyTest, unittest2.TestCase):
    def _register_database(self, settings_class):
        # Registry.register(Database, InMemoryDatabase())
        return settings_class.clone(
            database=InMemoryDatabase(key_builder=settings_class.key_builder))


class GreedyNumpyOnDiskTest(BaseNumpyTest, unittest2.TestCase):
    def _register_database(self, settings_class):
        self._dir = TempDir()
        return settings_class.clone(database=FileSystemDatabase(
            path=self._dir.path,
            key_builder=settings_class.key_builder))

    def tearDown(self):
        self._dir.cleanup()


class GreedyNumpyLmdbTest(BaseNumpyTest, unittest2.TestCase):
    def _register_database(self, settings_class):
        self._dir = TempDir()
        # Registry.register(Database, LmdbDatabase( \
        #         path=self._dir.path, map_size=10000000))
        return settings_class.clone(database=LmdbDatabase(
            path=self._dir.path,
            map_size=10000000,
            key_builder=settings_class.key_builder))

    def tearDown(self):
        self._dir.cleanup()


class StreamingNumpyTest(BaseNumpyTest, unittest2.TestCase):
    def _register_database(self, settings_class):
        # Registry.register(Database, InMemoryDatabase())
        return settings_class.clone(
            database=InMemoryDatabase(key_builder=settings_class.key_builder))

    def _build_doc(self):
        class Doc(BaseModel, self.Settings):
            feat = NumpyFeature(
                PassThrough,
                store=True,
                decoder=StreamingNumpyDecoder(n_examples=3))

        return Doc

    def _restore(self, data):
        return np.concatenate(list(data))


class StreamingNumpyOnDiskTest(BaseNumpyTest, unittest2.TestCase):
    def _register_database(self, settings_class):
        self._dir = TempDir()
        #Registry.register(Database, FileSystemDatabase(path=self._dir.path))
        return settings_class.clone(database=FileSystemDatabase(
            path=self._dir.path,
            key_builder=settings_class.key_builder))

    def tearDown(self):
        self._dir.cleanup()

    def _build_doc(self):
        class Doc(BaseModel, self.Settings):
            feat = NumpyFeature(
                PassThrough,
                store=True,
                decoder=StreamingNumpyDecoder(n_examples=3))

        return Doc

    def _restore(self, data):
        return np.concatenate(list(data))


class StreamingNumpyLmdbTest(BaseNumpyTest, unittest2.TestCase):
    def _register_database(self, settings_class):
        self._dir = TempDir()
        # Registry.register(Database, LmdbDatabase( \
        #         path=self._dir.path, map_size=10000000))
        return settings_class.clone(database=LmdbDatabase(
            path=self._dir.path,
            map_size=10000000,
            key_builder=settings_class.key_builder))

    def tearDown(self):
        self._dir.cleanup()

    def _build_doc(self):
        class Doc(BaseModel, self.Settings):
            feat = NumpyFeature(
                PassThrough,
                store=True,
                decoder=StreamingNumpyDecoder(n_examples=3))

        return Doc

    def _restore(self, data):
        return np.concatenate(list(data))
