import unittest2

try:
    import numpy as np
    from nmpy import NumpyFeature, StreamingNumpyDecoder, PackedNumpyEncoder
except ImportError:
    np = None

from persistence import PersistenceSettings
from data import *
from model import BaseModel
from lmdbstore import LmdbDatabase
from extractor import Node
from tempfile import mkdtemp
from shutil import rmtree


class PassThrough(Node):
    def __init__(self, needs=None):
        super(PassThrough, self).__init__(needs=needs)

    def _process(self, data):
        yield data


class BaseNumpyTest(object):
    def setUp(self):
        if np is None:
            self.skipTest('numpy is not available')

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
            packed = NumpyFeature(
                    PassThrough,
                    needs=feat,
                    encoder=PackedNumpyEncoder,
                    store=True)

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

    def test_can_store_and_retrieve_packed_array(self):
        cls = self._build_doc()
        arr = np.zeros((10, 9))
        _id = cls.process(feat=arr)
        doc = cls(_id)
        recovered = self._restore(doc.packed)
        self.assertEqual(np.uint8, recovered.dtype)
        self.assertEqual((10, 2), recovered.shape)

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
        return settings_class.clone(
            database=InMemoryDatabase(key_builder=settings_class.key_builder))


class GreedyNumpyOnDiskTest(BaseNumpyTest, unittest2.TestCase):
    def _register_database(self, settings_class):
        self._dir = mkdtemp()
        return settings_class.clone(database=FileSystemDatabase(
            path=self._dir,
            key_builder=settings_class.key_builder))

    def tearDown(self):
        rmtree(self._dir)


class GreedyNumpyLmdbTest(BaseNumpyTest, unittest2.TestCase):
    def _register_database(self, settings_class):
        self._dir = mkdtemp()
        return settings_class.clone(database=LmdbDatabase(
            path=self._dir,
            map_size=10000000,
            key_builder=settings_class.key_builder))

    def tearDown(self):
        rmtree(self._dir)


class StreamingNumpyTest(BaseNumpyTest, unittest2.TestCase):
    def _register_database(self, settings_class):
        return settings_class.clone(
            database=InMemoryDatabase(key_builder=settings_class.key_builder))

    def _build_doc(self):
        class Doc(BaseModel, self.Settings):
            feat = NumpyFeature(
                PassThrough,
                store=True,
                decoder=StreamingNumpyDecoder(n_examples=3))
            packed = NumpyFeature(
                PassThrough,
                needs=feat,
                encoder=PackedNumpyEncoder,
                decoder=StreamingNumpyDecoder(n_examples=3),
                store=True)

        return Doc

    def _restore(self, data):
        return np.concatenate(list(data))


class StreamingNumpyOnDiskTest(BaseNumpyTest, unittest2.TestCase):
    def _register_database(self, settings_class):
        self._dir = mkdtemp()
        return settings_class.clone(database=FileSystemDatabase(
            path=self._dir,
            key_builder=settings_class.key_builder))

    def tearDown(self):
        rmtree(self._dir)

    def _build_doc(self):
        class Doc(BaseModel, self.Settings):
            feat = NumpyFeature(
                PassThrough,
                store=True,
                decoder=StreamingNumpyDecoder(n_examples=3))
            packed = NumpyFeature(
                PassThrough,
                needs=feat,
                encoder=PackedNumpyEncoder,
                decoder=StreamingNumpyDecoder(n_examples=3),
                store=True)

        return Doc

    def _restore(self, data):
        return np.concatenate(list(data))


class StreamingNumpyLmdbTest(BaseNumpyTest, unittest2.TestCase):
    def _register_database(self, settings_class):
        self._dir = mkdtemp()
        return settings_class.clone(database=LmdbDatabase(
            path=self._dir,
            map_size=10000000,
            key_builder=settings_class.key_builder))

    def tearDown(self):
        rmtree(self._dir)

    def _build_doc(self):
        class Doc(BaseModel, self.Settings):
            feat = NumpyFeature(
                PassThrough,
                store=True,
                decoder=StreamingNumpyDecoder(n_examples=3))
            packed = NumpyFeature(
                    PassThrough,
                    needs=feat,
                    encoder=PackedNumpyEncoder,
                    decoder=StreamingNumpyDecoder(n_examples=3),
                    store=True)

        return Doc

    def _restore(self, data):
        return np.concatenate(list(data))
