import unittest2
from feature import Feature, JSONFeature
from lmdbstore import LmdbDatabase
from model import BaseModel
from persistence import PersistenceSettings, UuidProvider, \
    StringDelimitedKeyBuilder
from test_integration import TextStream, Tokenizer, WordCount
from tempfile import mkdtemp
from multiprocessing import Pool


class Settings(PersistenceSettings):
    id_provider = UuidProvider()
    key_builder = StringDelimitedKeyBuilder()
    database = LmdbDatabase(
        path=mkdtemp(),
        map_size=10000000,
        key_builder=key_builder)


class D(BaseModel, Settings):
    stream = Feature(TextStream, store=True)
    words = Feature(Tokenizer, needs=stream, store=False)
    count = JSONFeature(WordCount, needs=words, store=True)


def get_count(_):
    return len(list(D.database.iter_ids()))


class MultiProcessTests(unittest2.TestCase):
    def test_can_list_ids_from_multiple_processes(self):
        D.process(stream='Here is some text')
        D.process(stream='Here is some more')
        pool = Pool(4)
        counts = pool.map(get_count, [_ for _ in xrange(10)])
        self.assertSequenceEqual([2] * 10, counts)
