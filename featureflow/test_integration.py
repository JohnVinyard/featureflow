import unittest2
from collections import defaultdict
import random
from requests.exceptions import HTTPError
import subprocess
import sys
import time

from var import Var
from extractor import NotEnoughData, Aggregator, Node, InvalidProcessMethod
from iteratornode import IteratorNode
from model import BaseModel, NoPersistenceSettingsError, ModelExistsError
from feature import \
    Feature, JSONFeature, CompressedFeature, ClobberJSONFeature, \
    ClobberPickleFeature
from data import *
from bytestream import ByteStream, ByteStreamFeature
from io import BytesIO
from util import chunked
from lmdbstore import LmdbDatabase
from decoder import Decoder
from persistence import PersistenceSettings
from tempfile import mkdtemp
from shutil import rmtree
import traceback

data_source = {
    'mary': 'mary had a little lamb little lamb little lamb',
    'humpty': 'humpty dumpty sat on a wall humpty dumpty had a great fall',
    'numbers': range(10),
    'cased': 'This is a test.',
    'lorem': 'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum'
}


class TextStream(Node):
    def __init__(self, chunksize=3, needs=None):
        super(TextStream, self).__init__(needs=needs)
        self._chunksize = chunksize

    def _process(self, data):
        try:
            flo = StringIO(data_source[data])
        except KeyError as e:
            if isinstance(data, str):
                flo = StringIO(data)
            else:
                raise e

        for chunk in chunked(flo, chunksize=self._chunksize):
            yield chunk


class MaryTextStream(Node):
    def __init__(self, needs=None):
        super(MaryTextStream, self).__init__(needs=needs)

    def _process(self, data):
        flo = StringIO(data_source['mary'])
        for chunk in chunked(flo, chunksize=3):
            yield chunk


class TimestampEmitter(Aggregator, Node):
    def __init__(self, version='', needs=None):
        super(TimestampEmitter, self).__init__(needs=needs)
        self._version = version

    @property
    def version(self):
        return self._version

    def _process(self, data):
        yield self._version


class ValidatesDependencies(Node):
    def __init__(self, needs=None):
        if not needs:
            raise ValueError('you must supply at least one dependency')
        super(ValidatesDependencies, self).__init__(needs=needs)

    def _process(self, data):
        yield data


class Echo(Node):
    def __init__(self, needs=None):
        super(Echo, self).__init__(needs=needs)


class Counter(Node):
    Count = 0

    def _process(self, data):
        Counter.Count += 1
        yield data


class Dam(Aggregator, Node):
    """
    Gather input until all has been received, and then dole it out in small
    chunks
    """

    def __init__(self, chunksize=3, needs=None):
        super(Dam, self).__init__(needs=needs)
        self._chunksize = chunksize
        self._cache = ''

    def _enqueue(self, data, pusher):
        self._cache += data

    def _process(self, data):
        flo = StringIO(data)
        for chunk in chunked(flo, chunksize=self._chunksize):
            yield chunk


class ToUpper(Node):
    def __init__(self, needs=None):
        super(ToUpper, self).__init__(needs=needs)

    def _process(self, data):
        yield data.upper()


class ToLower(Node):
    def __init__(self, needs=None):
        super(ToLower, self).__init__(needs=needs)

    def _process(self, data):
        yield data.lower()


class CharacterCountNonGeneratorProcessMethod(Node):
    def __init__(self, needs=None):
        super(CharacterCountNonGeneratorProcessMethod, self).__init__(
            needs=needs)

    def _process(self, data):
        return len(data)


class Total(Aggregator, Node):
    def __init__(self, needs=None):
        super(Total, self).__init__(needs=needs)
        self._cache = 0

    def _enqueue(self, data, pusher):
        self._cache += data

    def _process(self, data):
        yield data


class Broken(Node):
    MESSAGE = uuid4().hex

    def __init__(
            self,
            raise_on_process=True,
            raise_on_enqueue=False,
            needs=None):

        super(Broken, self).__init__(needs=needs)
        self.raise_on_enqueue = raise_on_enqueue
        self.raise_on_process = raise_on_process

    def _enqueue(self, data, pusher):
        if self.raise_on_enqueue:
            raise TypeError(Broken.MESSAGE)
        super(Broken, self)._enqueue(data, pusher)

    def _process(self, data):
        if self.raise_on_process:
            raise Exception(Broken.MESSAGE)
        yield data


class TheLastWord(Node):
    def __init__(self, needs=None):
        super(TheLastWord, self).__init__(needs=needs)

    def _process(self, data):
        yield data.upper()

    def _last_chunk(self):
        yield 'final'


class Contrarion(Node):
    def __init__(self, needs=None):
        super(Contrarion, self).__init__(needs=needs)
        self._op = None

    def _first_chunk(self, data):
        if data[0].isupper():
            self._op = lambda x: x.lower()
        else:
            self._op = lambda x: x.upper()
        return data

    def _process(self, data):
        yield self._op(data)


class Concatenate(Aggregator, Node):
    def __init__(self, needs=None):
        super(Concatenate, self).__init__(needs=needs)
        self._cache = defaultdict(str)

    def _enqueue(self, data, pusher):
        self._cache[id(pusher)] += data

    def _process(self, data):
        yield ''.join((data[id(n)] for n in self._needs.values()))


class ExplicitConcatenate(Aggregator, Node):
    def __init__(self, blarg=None, needs=None):
        super(ExplicitConcatenate, self).__init__(needs=needs)
        self.blarg = blarg
        self._cache = defaultdict(str)

    def _enqueue(self, data, pusher):
        k = self._dependency_name(pusher)
        self._cache[k] += data

    def _last_chunk(self):
        if self.blarg:
            yield 'BLARG!'
        else:
            yield ''

    def _process(self, data):
        lhs = data['lhs']
        rhs = data['rhs']
        yield lhs + rhs


class UpperAndLower(Node):
    def __init__(self, needs=None):
        super(UpperAndLower, self).__init__(needs=needs)

    def _process(self, data):
        yield dict(upper=data.upper(), lower=data.lower())


class WordCountAggregator(Aggregator, Node):
    def __init__(self, needs=None):
        super(WordCountAggregator, self).__init__(needs=needs)
        self._cache = defaultdict(int)

    def _enqueue(self, data, pusher):
        for k, v in data.iteritems():
            self._cache[k.lower()] += v


class SumUp(Node):
    def __init__(self, needs=None):
        super(SumUp, self).__init__(needs=needs)
        self._cache = dict()

    def _enqueue(self, data, pusher):
        self._cache[id(pusher)] = data

    def _dequeue(self):
        if len(self._cache) != len(self._needs):
            raise NotEnoughData()
        v = self._cache
        self._cache = dict()
        return v

    def _process(self, data):
        results = [str(sum(x)) for x in zip(*data.itervalues())]
        yield ''.join(results)


class EagerConcatenate(Node):
    def __init__(self, needs=None):
        super(EagerConcatenate, self).__init__(needs=needs)
        self._cache = dict()

    def _enqueue(self, data, pusher):
        self._cache[id(pusher)] = data

    def _dequeue(self):
        v, self._cache = self._cache, dict()
        return v

    def _process(self, data):
        yield ''.join(data.itervalues())


class NumberStream(Node):
    def __init__(self, needs=None):
        super(NumberStream, self).__init__(needs=needs)

    def _process(self, data):
        l = data_source[data]
        for i in xrange(0, len(l), 3):
            yield l[i: i + 3]


class Add(Node):
    def __init__(self, rhs=1, needs=None):
        super(Add, self).__init__(needs=needs)
        self._rhs = rhs

    def _process(self, data):
        yield [c + self._rhs for c in data]


class Tokenizer(Node):
    def __init__(self, needs=None):
        super(Tokenizer, self).__init__(needs=needs)
        self._cache = ''

    def _enqueue(self, data, pusher):
        self._cache += data

    def _finalize(self, pusher):
        self._cache += ' '

    def _dequeue(self):
        last_index = self._cache.rfind(' ')
        if last_index == -1:
            raise NotEnoughData()
        current = self._cache[:last_index + 1]
        self._cache = self._cache[last_index + 1:]
        return current

    def _process(self, data):
        yield filter(lambda x: x, data.split(' '))


class WordCount(Aggregator, Node):
    def __init__(self, needs=None):
        super(WordCount, self).__init__(needs=needs)
        self._cache = defaultdict(int)

    def _enqueue(self, data, pusher):
        for word in data:
            self._cache[word.lower()] += 1


class IncrementalWordCount(Node):
    def __init__(self, needs=None, raise_for_chunk=None):
        super(IncrementalWordCount, self).__init__(needs=needs)
        self.count = defaultdict(int)
        self.raise_for_chunk = raise_for_chunk
        self._chunk = 0

    def _process(self, data):
        if self._chunk == self.raise_for_chunk:
            raise Exception('Raising for chunk {c}'.format(c=self._chunk))

        for word in data:
            self.count[word.lower()] += 1
        yield self.count
        self._chunk += 1


class FeatureAggregator(Node):
    def __init__(self, cls=None, feature=None, needs=None):
        super(FeatureAggregator, self).__init__(needs=needs)
        self._cls = cls
        self._feature = feature

    def _process(self, data):
        db = data
        for key in db.iter_ids():
            try:
                doc = self._cls(key)
                yield getattr(doc, self._feature.key)
            except:
                pass


# TODO: Move these classes into the tests, where possible
class Document(BaseModel):
    stream = Feature(TextStream, store=True)
    words = Feature(Tokenizer, needs=stream, store=False)
    count = JSONFeature(WordCount, needs=words, store=False)


class Document2(BaseModel):
    stream = Feature(TextStream, store=False)
    words = Feature(Tokenizer, needs=stream, store=False)
    count = JSONFeature(WordCount, needs=words, store=True)


class Doc4(BaseModel):
    stream = Feature(TextStream, chunksize=10, store=False)
    smaller = Feature(TextStream, needs=stream, chunksize=3, store=True)


class MultipleRoots(BaseModel):
    stream1 = Feature(TextStream, chunksize=3, store=False)
    stream2 = Feature(TextStream, chunksize=3, store=False)
    cat = Feature(EagerConcatenate, needs=[stream1, stream2], store=True)


class BaseTest(object):
    def test_clobber_feature_does_not_delete_previous_iteration_of_data(self):

        settings = self.Settings.clone(
            id_provider=UserSpecifiedIdProvider(key='_id'))

        class Document(BaseModel, settings):
            stream = Feature(TextStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)
            count = ClobberJSONFeature(
                IncrementalWordCount,
                needs=words,
                store=True,
                raise_for_chunk=2)

        _id = 'doc_id'
        try:
            Document.process(stream='lorem', _id=_id)
        except Exception:
            pass

        count_key = Document.count.feature_key(_id, Document)
        self.assertTrue(
            count_key in Document.database,
            'Database should still contain {count_key}'.format(**locals()))

    def test_can_overwrite_instead_of_append_json_feature_data(self):
        class Document(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)
            count = ClobberJSONFeature(
                IncrementalWordCount, needs=words, store=True)

        _id = Document.process(stream='lorem')
        doc = Document(_id)
        self.assertEqual(3, doc.count['ut'])

    def test_can_overwrite_instead_of_append_pickle_feature_data(self):
        class Document(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)
            count = ClobberPickleFeature(
                IncrementalWordCount, needs=words, store=True)

        _id = Document.process(stream='lorem')
        doc = Document(_id)
        self.assertEqual(3, doc.count['ut'])

    def test_can_split_dict_feature(self):
        class Document(BaseModel, self.Settings):
            stream = Feature(TextStream, store=False)
            both = Feature(UpperAndLower, needs=stream, store=False)
            upper = Feature(Echo, needs=both.aspect('upper'), store=True)
            lower = Feature(Echo, needs=both.aspect('lower'), store=True)

        _id = Document.process(stream='cased')
        doc = Document(_id)
        self.assertEqual('this is a test.', doc.lower.read())
        self.assertEqual('THIS IS A TEST.', doc.upper.read())

    def test_can_split_on_dict_feature_unstored(self):
        class Document(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            both = Feature(UpperAndLower, needs=stream, store=False)
            upper = Feature(Echo, needs=both.aspect('upper'), store=False)
            lower = Feature(Echo, needs=both.aspect('lower'), store=False)

        _id = Document.process(stream='cased')
        doc = Document(_id)
        self.assertEqual('this is a test.', doc.lower.read())
        self.assertEqual('THIS IS A TEST.', doc.upper.read())

    def test_can_nest_aspect_call_inside_of_dict_dependency(self):
        class Document(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            both = Feature(UpperAndLower, needs=stream, store=False)
            upper = Feature(ToUpper, needs=stream, store=False)
            cat = Feature(
                ExplicitConcatenate,
                needs=dict(lhs=upper, rhs=both.aspect('lower')),
                store=False)

        keyname = 'cased'
        _id = Document.process(stream=keyname)
        doc = Document(_id)

        self.assertEqual('THIS IS A TEST.this is a test.', doc.cat.read())

    def test_feature_with_aspect_dependencies_can_also_have_arguments(self):
        class Document(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            both = Feature(UpperAndLower, needs=stream, store=False)
            upper = Feature(ToUpper, needs=stream, store=False)
            cat = Feature(
                ExplicitConcatenate,
                blarg=True,
                needs=dict(lhs=upper, rhs=both.aspect('lower')),
                store=False)

        keyname = 'cased'
        _id = Document.process(stream=keyname)
        doc = Document(_id)

        self.assertEqual('THIS IS A TEST.this is a test.BLARG!', doc.cat.read())

    def test_can_have_multiple_explicit_dependencies(self):
        class Split(BaseModel, self.Settings):
            stream = Feature(TextStream, store=False)
            uppercase = Feature(ToUpper, needs=stream, store=True)
            lowercase = Feature(ToLower, needs=stream, store=True)
            cat = Feature(
                ExplicitConcatenate,
                needs=dict(lhs=uppercase, rhs=lowercase),
                store=False)

        keyname = 'cased'
        _id = Split.process(stream=keyname)
        doc = Split(_id)

        self.assertEqual('THIS IS A TEST.this is a test.', doc.cat.read())

        class Split2(BaseModel, self.Settings):
            stream = Feature(TextStream, store=False)
            uppercase = Feature(ToUpper, needs=stream, store=True)
            lowercase = Feature(ToLower, needs=stream, store=True)
            cat = Feature(
                ExplicitConcatenate,
                needs=dict(lhs=lowercase, rhs=uppercase),
                store=False)

        _id = Split2.process(stream=keyname)
        doc = Split2(_id)

        self.assertEqual('this is a test.THIS IS A TEST.', doc.cat.read())

    def test_can_have_multiple_explicit_dependencies_stored_feature(self):
        class Split(BaseModel, self.Settings):
            stream = Feature(TextStream, store=False)
            uppercase = Feature(ToUpper, needs=stream, store=True)
            lowercase = Feature(ToLower, needs=stream, store=True)
            cat = Feature(
                ExplicitConcatenate,
                needs=dict(lhs=uppercase, rhs=lowercase),
                store=True)

        keyname = 'cased'
        _id = Split.process(stream=keyname)
        doc = Split(_id)

        self.assertEqual('THIS IS A TEST.this is a test.', doc.cat.read())

        class Split2(BaseModel, self.Settings):
            stream = Feature(TextStream, store=False)
            uppercase = Feature(ToUpper, needs=stream, store=True)
            lowercase = Feature(ToLower, needs=stream, store=True)
            cat = Feature(
                ExplicitConcatenate,
                needs=dict(lhs=lowercase, rhs=uppercase),
                store=True)

        _id = Split2.process(stream=keyname)
        doc = Split2(_id)

        self.assertEqual('this is a test.THIS IS A TEST.', doc.cat.read())

    def test_can_compute_feature_directly_with_lambda(self):
        class Split(BaseModel, self.Settings):
            stream = Feature(TextStream, store=False)
            uppercase = Feature(lambda x: x.upper(), needs=stream, store=True)

        _id = Split.process(stream='cased')
        doc = Split(_id)

        self.assertEqual('THIS IS A TEST.', doc.uppercase.read())

    def test_functional_node_returning_generator_replaces_process_method(self):

        def mutate(x):
            return x.upper()

        class Split(BaseModel, self.Settings):
            stream = Feature(TextStream, store=False)
            passthrough = Feature(
                lambda x: (y.lower() for y in x), needs=stream, store=False)
            uppercase = Feature(mutate, needs=passthrough, store=True)

        _id = Split.process(stream='cased')
        doc = Split(_id)

        self.assertEqual('THIS IS A TEST.', doc.uppercase.read())

    def test_functional_node_raises_when_code_changes_and_cannot_recompute(self):

        def mutate(x):
            return x.upper()

        class Split(BaseModel, self.Settings):
            stream = Feature(TextStream, store=False)
            uppercase = Feature(mutate, needs=stream, store=True)

        _id = Split.process(stream='cased')
        doc = Split(_id)

        self.assertEqual('THIS IS A TEST.', doc.uppercase.read())

        def mutate(x):
            return x.lower()

        class Split(BaseModel, self.Settings):
            stream = Feature(TextStream, store=False)
            uppercase = Feature(mutate, needs=stream, store=True)

        doc = Split(_id)

        self.assertRaises(AttributeError, lambda: doc.uppercase.read())

    def test_functional_node_is_recomputed_when_code_changes(self):

        def mutate(x):
            return x.upper()

        class Split(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            uppercase = Feature(mutate, needs=stream, store=True)

        _id = Split.process(stream='cased')
        doc = Split(_id)

        self.assertEqual('THIS IS A TEST.', doc.uppercase.read())

        def mutate(x):
            return x.lower()

        class Split(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            uppercase = Feature(mutate, needs=stream, store=True)

        doc = Split(_id)

        self.assertEqual('this is a test.', doc.uppercase.read())

    def test_functional_node_is_recomputed_when_code_changes(self):

        def mutate(x):
            return x.upper()

        class Split(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            uppercase = Feature(mutate, needs=stream, store=True)

        _id = Split.process(stream='cased')
        doc = Split(_id)

        self.assertEqual('THIS IS A TEST.', doc.uppercase.read())

        def mutate(x):
            return x.lower()

        class Split(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            uppercase = Feature(mutate, needs=stream, store=True)

        doc = Split(_id)

        self.assertEqual('this is a test.', doc.uppercase.read())

    def test_functional_node_is_recomputed_when_free_variable_changes(self):

        a = 10
        b = 20

        def mutate(x):
            return x.upper() + str(a) + str(b)

        class Split(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True, chunksize=100)
            uppercase = Feature(
                mutate,
                needs=stream,
                store=True,
                closure_fingerprint=lambda d: str(d['a']))

        _id = Split.process(stream='cased')
        doc = Split(_id)

        self.assertEqual('THIS IS A TEST.1020', doc.uppercase.read())

        a = 20

        doc = Split(_id)

        self.assertEqual('THIS IS A TEST.2020', doc.uppercase.read())

    def test_can_compute_feature_directly_with_callable(self):
        class FancyUpperCase(object):
            def __call__(self, x):
                return x.upper()

        class Split(BaseModel, self.Settings):
            stream = Feature(TextStream, store=False)
            uppercase = Feature(FancyUpperCase(), needs=stream, store=True)

        _id = Split.process(stream='cased')
        doc = Split(_id)

        self.assertEqual('THIS IS A TEST.', doc.uppercase.read())

    def test_can_check_if_document_exists_using_static_id(self):
        settings = self.Settings.clone(
            id_provider=StaticIdProvider('SOME_ID'))

        class Document(BaseModel, settings):
            stream = Feature(TextStream, store=True)
            dam = Feature(Dam, needs=stream, store=False)
            words = Feature(Tokenizer, needs=dam, store=False)
            count = JSONFeature(WordCount, needs=words, store=False)

        self.assertFalse(Document.exists())
        Document.process(stream='humpty', _id='blah')
        self.assertTrue(Document.exists())

    def test_can_check_if_document_exists_using_explicit_feature(self):
        settings = self.Settings.clone(
            id_provider=UserSpecifiedIdProvider(key='_id'))

        class Document(BaseModel, settings):
            stream = Feature(TextStream, store=True)
            dam = Feature(Dam, needs=stream, store=False)
            words = Feature(Tokenizer, needs=dam, store=False)
            count = JSONFeature(WordCount, needs=words, store=False)

        _id = Document.process(stream='humpty', _id='blah')
        self.assertTrue(Document.exists(_id, Document.stream))

    def test_exists_raises_for_non_static_id_provider_and_no_id_provided(self):
        settings = self.Settings.clone(
            id_provider=UserSpecifiedIdProvider(key='_id'))

        class Document(BaseModel, settings):
            stream = Feature(TextStream, store=True)
            dam = Feature(Dam, needs=stream, store=False)
            words = Feature(Tokenizer, needs=dam, store=False)
            count = JSONFeature(WordCount, needs=words, store=False)

        Document.process(stream='humpty', _id='blah')
        self.assertRaises(ValueError, lambda: Document.exists())

    def test_document_does_not_exist(self):
        settings = self.Settings.clone(
            id_provider=UserSpecifiedIdProvider(key='_id'))

        class Document(BaseModel, settings):
            stream = Feature(TextStream, store=True)
            dam = Feature(Dam, needs=stream, store=False)
            words = Feature(Tokenizer, needs=dam, store=False)
            count = JSONFeature(WordCount, needs=words, store=False)

        self.assertFalse(Document.exists('blah'))

    def test_can_check_if_document_exists_without_supply_feature(self):
        settings = self.Settings.clone(
            id_provider=UserSpecifiedIdProvider(key='_id'))

        class Document(BaseModel, settings):
            stream = Feature(TextStream, store=True)
            dam = Feature(Dam, needs=stream, store=False)
            words = Feature(Tokenizer, needs=dam, store=False)
            count = JSONFeature(WordCount, needs=words, store=False)

        _id = Document.process(stream='humpty', _id='blah')
        self.assertTrue(Document.exists(_id))

    def test_existence_check_fails_if_supplied_feature_is_not_stored(self):
        settings = self.Settings.clone(
            id_provider=UserSpecifiedIdProvider(key='_id'))

        class Document(BaseModel, settings):
            stream = Feature(TextStream, store=True)
            dam = Feature(Dam, needs=stream, store=False)
            words = Feature(Tokenizer, needs=dam, store=False)
            count = JSONFeature(WordCount, needs=words, store=False)

        _id = Document.process(stream='humpty', _id='blah')
        self.assertRaises(
            ValueError, lambda: Document.exists(_id, Document.count))

    def test_get_sane_stack_trace_when_node_raises_on_enqueue(self):
        class D(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)
            broken = Feature(
                Broken,
                raise_on_process=False,
                raise_on_enqueue=True,
                needs=words,
                store=False)
            count = JSONFeature(WordCount, needs=broken, store=True)

        try:
            D.process(stream='mary')
        except Exception:
            _, _, tb = sys.exc_info()
            items = traceback.extract_tb(tb)
            self.assertEqual('raise TypeError(Broken.MESSAGE)', items[-1][-1])
            return

        self.fail('Exception should have been raised')

    def test_get_sane_stack_trace_when_node_raises(self):
        class D(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)
            broken = Feature(Broken, needs=words, store=False)
            count = JSONFeature(WordCount, needs=broken, store=True)

        try:
            D.process(stream='mary')
        except Exception:
            _, _, tb = sys.exc_info()
            items = traceback.extract_tb(tb)
            self.assertEqual('raise Exception(Broken.MESSAGE)', items[-1][-1])
            return

        self.fail('Exception should have been raised')

    def test_can_iter_over_document_class(self):
        class D(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)
            count = JSONFeature(WordCount, needs=words, store=True)

        D.process(stream='mary')
        D.process(stream='humpty')
        D.process(stream='cased')

        l = list(doc for doc in D)
        self.assertEqual(3, len(l))
        self.assertTrue(all(isinstance(x, D) for x in l))

    def test_can_choose_random_document(self):
        class D(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)
            count = JSONFeature(WordCount, needs=words, store=True)

        D.process(stream='mary')
        D.process(stream='humpty')
        D.process(stream='cased')

        random = D.random()
        self.assertIsInstance(random, D)

    def test_can_use_node_that_validates_its_dependency_list(self):
        class D1(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)
            count = JSONFeature(WordCount, needs=words, store=True)
            timestamp = JSONFeature(
                TimestampEmitter,
                version='1',
                needs=stream,
                store=True)
            validated = Feature(ValidatesDependencies, needs=stream, store=True)

        _id = D1.process(stream='mary')
        self.assertTrue(_id)

    def test_recomputes_when_necessary(self):
        class D1(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)
            count = JSONFeature(WordCount, needs=words, store=True)
            timestamp = JSONFeature(
                TimestampEmitter,
                version='1',
                needs=stream,
                store=True)

        class D2(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)
            count = JSONFeature(WordCount, needs=words, store=True)
            timestamp = JSONFeature(
                TimestampEmitter,
                version='2',
                needs=stream,
                store=True)

        _id = D1.process(stream='mary')
        v1 = D1(_id).timestamp
        v2 = D2(_id).timestamp
        self.assertNotEqual(v1, v2)

    def test_can_force_recompute_without_reading_back_data(self):
        class D1(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)
            count = JSONFeature(WordCount, needs=words, store=True)
            timestamp = JSONFeature(
                TimestampEmitter,
                version='1',
                needs=stream,
                store=True)

        class D2(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)
            count = JSONFeature(WordCount, needs=words, store=True)
            timestamp = JSONFeature(
                TimestampEmitter,
                version='2',
                needs=stream,
                store=True)

        _id = D1.process(stream='mary')
        D2.timestamp.compute(_id=_id, persistence=D2)

        database = self.Settings.database
        key_builder = self.Settings.key_builder
        key = key_builder.build(_id, 'timestamp', '2')
        self.assertTrue(key in database)

    def test_can_iterate_over_database(self):
        class D(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)
            count = JSONFeature(WordCount, needs=words, store=True)

        D.process(stream='mary')
        D.process(stream='humpty')
        D.process(stream='cased')

        self.assertEqual(3, len(list(self.Settings.database)))

    def test_can_use_iterator_node(self):
        iterable = chunked(StringIO(data_source['mary']), chunksize=3)

        class D(BaseModel, self.Settings):
            stream = Feature(IteratorNode, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)
            count = JSONFeature(WordCount, needs=words, store=True)

        _id = D.process(stream=iterable)
        doc = D(_id)
        self.assertEqual(data_source['mary'], doc.stream.read())

    def test_keys_are_removed_when_exception_is_thrown_during_processing(self):
        class D(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)
            count = JSONFeature(WordCount, needs=words, store=True)

        _id1 = D.process(stream='mary')
        try:
            D.process(stream=10)
        except KeyError:
            pass

        _ids = list(self.Settings.database.iter_ids())
        self.assertTrue(_id1 in _ids)
        self.assertEqual(1, len(_ids))

    def test_graph_including_non_generator_process_method_raises(self):
        class D(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            length = Feature(
                CharacterCountNonGeneratorProcessMethod,
                needs=stream,
                store=True)
            total = Feature(Total, needs=length, store=True)

        self.assertRaises(
            InvalidProcessMethod, lambda: D.process(stream='mary'))

    def test_can_use_string_for_remote_url(self):
        class D(BaseModel, self.Settings):
            stream = Feature(ByteStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)
            count = JSONFeature(WordCount, needs=words, store=True)

        try:
            devnull = open(os.devnull, 'w')
            p = subprocess.Popen(
                [sys.executable, '-m', 'SimpleHTTPServer', '9765'],
                stdout=devnull,
                stderr=devnull)
            time.sleep(0.25)
            url = 'http://localhost:9765/{path}'.format(path=uuid4().hex)
            self.assertRaises(
                HTTPError,
                lambda: D.process(stream=url))
        finally:
            p.kill()

    def test_can_get_size_in_bytes_of_key(self):
        class D(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)
            count = JSONFeature(WordCount, needs=words, store=True)

        _id = D.process(stream='mary')
        key = self.Settings.key_builder.build(
            _id, D.stream.key, D.stream.version)
        self.assertEqual(
            len(data_source['mary']), self.Settings.database.size(key))

    def test_key_error_if_asking_for_size_of_unknown_key(self):
        class D(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)
            count = JSONFeature(WordCount, needs=words, store=True)

        _id = D.process(stream='mary')
        key = self.Settings.key_builder.build(_id, 'something', 'else')
        self.assertRaises(KeyError, lambda: self.Settings.database.size(key))

    def test_can_retrieve_feature_with_deep_inheritance_hierarchy(self):
        class D1(BaseModel):
            stream = Feature(TextStream, store=False)
            echo = Feature(Echo, needs=stream, store=True)

        class D2(D1):
            words = Feature(Tokenizer, needs=D1.stream, store=False)

        class D3(D2):
            count = JSONFeature(WordCount, needs=D2.words, store=True)

        class Document(D3, self.Settings):
            pass

        _id = Document.process(stream='mary')
        doc = Document(_id)
        self.assertEqual(data_source['mary'], doc.echo.read())

    def test_features_are_inherited(self):
        class D1(BaseModel):
            stream = Feature(TextStream, store=True)

        class D2(D1):
            words = Feature(Tokenizer, needs=D1.stream, store=True)

        class D3(D2):
            count = JSONFeature(WordCount, needs=D2.words, store=True)

        class Document(D3, self.Settings):
            pass

        self.assertTrue('stream' in Document.features)

    def test_can_decode_bytestream_feature(self):
        class Doc(BaseModel, self.Settings):
            raw = ByteStreamFeature(ByteStream, chunksize=3, store=True)

        class HasUri(object):
            def __init__(self, uri):
                super(HasUri, self).__init__()
                self.uri = uri

        text = data_source['mary']
        _id = Doc.process(raw=HasUri(BytesIO(text)))
        doc = Doc(_id)
        self.assertEqual(text, ''.join(doc.raw))

    def test_can_use_alternate_decoder_for_stored_feature(self):
        class Doc(Document2, self.Settings):
            pass

        _id = Doc.process(stream='humpty')
        count_as_text = Doc.count(
            _id=_id, decoder=Decoder(), persistence=self.Settings).read()
        self.assertTrue(isinstance(count_as_text, str))
        self.assertTrue('{' in count_as_text)

    def test_can_user_alternate_decoder_for_unstored_feature(self):
        class D(BaseModel, self.Settings):
            stream = Feature(TextStream, store=False)
            words = Feature(Tokenizer, needs=stream, store=False)
            count = JSONFeature(WordCount, needs=words, store=True)

        _id = D.process(stream='humpty')
        count_as_text = D.count(
            _id=_id, decoder=Decoder(), persistence=self.Settings).read()
        self.assertTrue(isinstance(count_as_text, str))
        self.assertTrue('{' in count_as_text)

    def test_initializes_on_first_chunk(self):
        class D(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            opposite = Feature(Contrarion, needs=stream, store=True)

        _id = D.process(stream='cased')
        doc = D(_id)
        self.assertTrue(doc.opposite.read().islower())

    def test_can_pass_bytes_io_to_bytestream(self):
        class ByteStreamDocument(BaseModel, self.Settings):
            stream = Feature(ByteStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)
            count = JSONFeature(WordCount, needs=words, store=False)

        class HasUri(object):
            def __init__(self, bio):
                self.uri = bio

        bio = BytesIO()
        bio.write('mary had a little lamb little lamb little lamb')
        huri = HasUri(bio)

        _id = ByteStreamDocument.process(stream=huri)
        doc = ByteStreamDocument(_id)
        self.assertEqual(3, doc.count['lamb'])

    def test_can_output_final_chunk(self):
        class Doc(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            final = Feature(TheLastWord, needs=stream, store=True)

        _id = Doc.process(stream='cased')
        doc = Doc(_id)
        self.assertEqual('THIS IS A TEST.final', doc.final.read())

    def test_unstored_feature_with_no_stored_dependents_is_not_computed_during_process(
            self):
        class D(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            copy = Feature(Counter, needs=stream, store=False)
            words = Feature(Tokenizer, needs=copy, store=False)
            count = JSONFeature(WordCount, needs=words, store=False)

        Counter.Count = 0
        D.process(stream='humpty')
        self.assertEqual(0, Counter.Count)

    def test_unstored_leaf_feature_is_not_computed_during_process(self):
        class D(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            copy = Feature(Counter, needs=stream, store=False)
            words = Feature(Tokenizer, needs=stream, store=False)
            count = JSONFeature(WordCount, needs=words, store=True)

        Counter.Count = 0
        D.process(stream='humpty')
        self.assertEqual(0, Counter.Count)

    def test_can_incrementally_build_document(self):
        class D1(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)

        _id = D1.process(stream='humpty')

        class D2(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)
            count = JSONFeature(WordCount, needs=words, store=True)

        # count should be computed and stored lazily
        doc = D2(_id)
        self.assertEqual(2, doc.count['a'])
        del doc

        db = self.Settings.database
        key_builder = self.Settings.key_builder
        key = key_builder.build(_id, 'count', D2.count.version)
        self.assertTrue(key in db)

        # count should be retrieved
        doc = D2(_id)
        self.assertEqual(2, doc.count['a'])

    def test_can_incrementally_build_document_with_two_new_stored_features(
            self):
        class D1(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)

        _id = D1.process(stream='humpty')

        class D2(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)
            count = JSONFeature(WordCount, needs=words, store=True)
            aggregate = JSONFeature(WordCountAggregator, needs=count,
                                    store=True)

        # count should be computed and stored lazily
        doc = D2(_id)
        self.assertEqual(2, doc.count['a'])
        del doc

        # count should be retrieved
        doc = D2(_id)
        self.assertEqual(2, doc.count['a'])
        del doc

        db = self.Settings.database
        key_builder = self.Settings.key_builder
        key = key_builder.build(_id, 'count', D2.count.version)
        self.assertTrue(key in db)

        # aggregate should be computed and stored lazily
        doc = D2(_id)
        self.assertEqual(2, doc.aggregate['a'])
        del doc

        key = key_builder.build(_id, 'aggregate', D2.aggregate.version)
        self.assertTrue(key in db)

        # aggregate should be retrieved
        doc = D2(_id)
        self.assertEqual(2, doc.aggregate['a'])

    def test_can_incrementally_build_document_by_calling_leaf_feature(self):
        class D1(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)

        _id = D1.process(stream='humpty')

        class D2(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            words = Feature(Tokenizer, needs=stream, store=False)
            count = JSONFeature(WordCount, needs=words, store=True)
            aggregate = JSONFeature(WordCountAggregator, needs=count,
                                    store=True)

        # aggregate should be computed and stored lazily
        doc = D2(_id)
        self.assertEqual(2, doc.aggregate['a'])
        del doc

        db = self.Settings.database
        key_builder = self.Settings.key_builder

        # Note that count was never called explicitly, but we should have stored
        # it just the same
        key = key_builder.build(_id, 'count', D2.count.version)
        self.assertTrue(key in db)
        key = key_builder.build(_id, 'aggregate', D2.aggregate.version)
        self.assertTrue(key in db)

        # count should be retrieved
        doc = D2(_id)
        self.assertEqual(2, doc.count['a'])
        del doc

        # aggregate should be retrieved
        doc = D2(_id)
        self.assertEqual(2, doc.aggregate['a'])

    def test_can_explicitly_specify_identifier(self):
        settings = self.Settings.clone(
            id_provider=UserSpecifiedIdProvider(key='_id'))

        class Document(BaseModel, settings):
            stream = Feature(TextStream, store=True)
            dam = Feature(Dam, needs=stream, store=False)
            words = Feature(Tokenizer, needs=dam, store=False)
            count = JSONFeature(WordCount, needs=words, store=False)

        _id = Document.process(stream='humpty', _id='blah')
        self.assertEqual('blah', _id)
        doc = Document(_id)
        self.assertEqual(2, doc.count['a'])

    def test_raises_when_processing_same_id_twice(self):
        settings = self.Settings.clone(
            id_provider=UserSpecifiedIdProvider(key='_id'))

        class Document(BaseModel, settings):
            stream = Feature(TextStream, store=True)
            dam = Feature(Dam, needs=stream, store=False)
            words = Feature(Tokenizer, needs=dam, store=False)
            count = JSONFeature(WordCount, needs=words, store=False)

        Document.process(stream='humpty', _id='blah', raise_if_exists=True)
        # processing the same id a second time should raise an error
        self.assertRaises(ModelExistsError, lambda: Document.process(
            stream='humpty', _id='blah', raise_if_exists=True))

    def test_can_have_multiple_producer_like_nodes(self):
        class Document(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            dam = Feature(Dam, needs=stream, store=False)
            words = Feature(Tokenizer, needs=dam, store=False)
            count = JSONFeature(WordCount, needs=words, store=False)

        _id = Document.process(stream='humpty')
        doc = Document(_id)
        self.assertEqual(2, doc.count['a'])

    def test_can_aggregate_word_counts_from_multiple_inputs(self):
        class Contrived(BaseModel, self.Settings):
            stream1 = Feature(TextStream, store=False)
            stream2 = Feature(TextStream, store=False)
            t1 = Feature(Tokenizer, needs=stream1, store=False)
            t2 = Feature(Tokenizer, needs=stream2, store=False)
            count1 = JSONFeature(WordCount, needs=t1, store=True)
            count2 = JSONFeature(WordCount, needs=t2, store=True)
            aggregate = JSONFeature( \
                WordCountAggregator, needs=[count1, count2], store=True)

        _id = Contrived.process(stream1='mary', stream2='humpty')
        doc = Contrived(_id)
        self.assertEqual(3, doc.aggregate['a'])

    def test_stored_features_are_not_rewritten_when_computing_dependent_feature(
            self):
        class Timestamp(Aggregator, Node):
            def __init__(self, needs=None):
                super(Timestamp, self).__init__(needs=needs)
                self._cache = ''

            def _enqueue(self, data, pusher):
                self._cache += data

            def _process(self, data):
                yield str(random.random())

        class Timestamps(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            t1 = Feature(Timestamp, needs=stream, store=True)
            t2 = Feature(Timestamp, needs=stream, store=False)
            cat = Feature( \
                Concatenate, needs=[t1, t2], store=False)

        _id = Timestamps.process(stream='cased')
        doc1 = Timestamps(_id)
        orig = doc1.t1.read()
        doc2 = Timestamps(_id)
        _ = doc2.cat.read()
        new = doc2.t1.read()
        self.assertEqual(orig, new)
        self.assertEqual(doc2.t1.read() + doc2.t1.read(), doc2.cat.read())

    def test_can_process_multiple_documents_and_then_aggregate_word_count(self):
        class Doc(Document, self.Settings):
            pass

        class DocumentWordCount(BaseModel, self.Settings):
            counts = Feature(
                FeatureAggregator,
                cls=Doc,
                feature=Doc.count,
                store=False)

            total_count = JSONFeature(
                WordCountAggregator,
                store=True,
                needs=counts)

        Doc.process(stream='mary')
        Doc.process(stream='humpty')
        _id3 = DocumentWordCount.process(
            counts=self.Settings.database)
        doc = DocumentWordCount(_id3)
        self.assertEqual(3, doc.total_count['a'])

    def test_can_use_single_document_database_for_aggregate_feature(self):
        class Doc(Document, self.Settings):
            pass

        class SingleDocumentDatabaseSettings(PersistenceSettings):
            _id = 'static'
            id_provider = StaticIdProvider(_id)
            key_builder = StringDelimitedKeyBuilder()
            database = InMemoryDatabase(key_builder=key_builder)

        class DocumentWordCount(BaseModel, SingleDocumentDatabaseSettings):
            counts = Feature(
                FeatureAggregator,
                cls=Doc,
                feature=Doc.count,
                store=False)

            total_count = JSONFeature(
                WordCountAggregator,
                store=True,
                needs=counts)

        Doc.process(stream='mary')
        Doc.process(stream='humpty')
        DocumentWordCount.process(counts=self.Settings.database)
        doc = DocumentWordCount()
        self.assertEqual(3, doc.total_count['a'])

    def test_document_with_multiple_roots(self):
        class Doc(MultipleRoots, self.Settings):
            pass

        _id = Doc.process(stream1='mary', stream2='humpty')
        doc = Doc(_id)
        data = doc.cat.read()
        # KLUDGE: Note that order is unknown, since we're using a dict
        self.assertTrue('mar' in data[:6])
        self.assertTrue('hum' in data[:6])
        self.assertEqual( \
            len(data_source['mary']) + len(data_source['humpty']),
            len(data))
        self.assertTrue(data.endswith('fall'))

    def test_smaller_chunks_downstream(self):
        class Doc(Doc4, self.Settings):
            pass

        _id = Doc.process(stream='mary')
        doc = Doc(_id)
        self.assertEqual(data_source['mary'], doc.smaller.read())

    def test_exception_is_thrown_if_all_kwargs_are_not_provided(self):
        class Doc(MultipleRoots, self.Settings):
            pass

        self.assertRaises(
            KeyError,
            lambda: Doc.process(stream1='mary'))

    def test_raises_when_no_settings_base_class_and_iter_called(self):
        self.assertRaises(NoPersistenceSettingsError, lambda: list(Document))

    def test_get_meaningful_error_when_no_settings_base_class_and_process_is_called(
            self):
        self.assertRaises(
            NoPersistenceSettingsError,
            lambda: Document.process(stream='mary'))

    def test_get_meaningful_error_when_no_settings_base_class_and_getattr_is_called(
            self):
        class Doc(Document, self.Settings):
            pass

        _id = Doc.process(stream='mary')
        doc = Document(_id)
        self.assertRaises(
            NoPersistenceSettingsError,
            lambda: doc.stream.read())

    def test_can_process_and_retrieve_stored_feature(self):
        class Doc(Document, self.Settings):
            pass

        _id = Doc.process(stream='mary')
        doc = Doc(_id)
        self.assertEqual(data_source['mary'], doc.stream.read())

    def test_can_correctly_decode_feature(self):
        class Doc(Document2, self.Settings):
            pass

        _id = Doc.process(stream='mary')
        doc = Doc(_id)
        self.assertTrue(isinstance(doc.count, dict))

    def test_can_retrieve_unstored_feature_when_dependencies_are_satisfied(
            self):
        class Doc(Document, self.Settings):
            pass

        _id = Doc.process(stream='humpty')
        doc = Doc(_id)
        d = doc.count
        self.assertEqual(2, d['humpty'])
        self.assertEqual(1, d['sat'])

    def test_cannot_retrieve_unstored_feature_when_dependencies_are_not_satisfied(
            self):
        class Doc(Document2, self.Settings):
            pass

        _id = Doc.process(stream='humpty')
        doc = Doc(_id)
        self.assertRaises(AttributeError, lambda: doc.stream)

    def test_feature_with_multiple_inputs(self):
        class Numbers(BaseModel, self.Settings):
            stream = Feature(NumberStream, store=False)
            add1 = Feature(Add, needs=stream, store=False, rhs=1)
            add2 = Feature(Add, needs=stream, store=False, rhs=1)
            sumup = Feature(SumUp, needs=[add1, add2], store=True)

        _id = Numbers.process(stream='numbers')
        doc = Numbers(_id)
        self.assertEqual('2468101214161820', doc.sumup.read())

    def test_feature_that_takes_a_variable(self):

        class Numbers(BaseModel, self.Settings):
            stream = Feature(NumberStream, store=False)
            add1 = Feature(Add, needs=stream, store=False, rhs=Var('rhs'))
            stringify = Feature(
                lambda x: ''.join(map(str, x)), needs=add1, store=True)

        _id = Numbers.process(stream='numbers', rhs=2)
        doc = Numbers(_id)
        self.assertEqual('234567891011', doc.stringify.read())

    def test_raises_if_necessary_variable_is_not_provided(self):
        class Numbers(BaseModel, self.Settings):
            stream = Feature(NumberStream, store=False)
            add1 = Feature(Add, needs=stream, store=False, rhs=Var('rhs'))
            stringify = Feature(
                lambda x: ''.join(map(str, x)), needs=add1, store=True)

        self.assertRaises(ValueError, lambda: Numbers.process(stream='numbers'))

    def test_feature_with_multiple_inputs_using_a_tuple(self):
        class Numbers(BaseModel, self.Settings):
            stream = Feature(NumberStream, store=False)
            add1 = Feature(Add, needs=stream, store=False, rhs=1)
            add2 = Feature(Add, needs=stream, store=False, rhs=1)
            sumup = Feature(SumUp, needs=(add1, add2), store=True)

        _id = Numbers.process(stream='numbers')
        doc = Numbers(_id)
        self.assertEqual('2468101214161820', doc.sumup.read())

    def test_unstored_feature_with_multiple_inputs_can_be_computed(self):
        class Doc3(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            uppercase = Feature(ToUpper, needs=stream, store=True)
            lowercase = Feature(ToLower, needs=stream, store=False)
            cat = Feature(
                Concatenate, needs=[uppercase, lowercase], store=False)

        _id = Doc3.process(stream='cased')
        doc = Doc3(_id)
        self.assertEqual('THIS IS A TEST.this is a test.', doc.cat.read())

    def test_can_read_computed_property_with_multiple_dependencies(self):
        class Split(BaseModel, self.Settings):
            stream = Feature(TextStream, store=False)
            uppercase = Feature(ToUpper, needs=stream, store=True)
            lowercase = Feature(ToLower, needs=stream, store=True)
            cat = Feature(
                Concatenate, needs=[uppercase, lowercase], store=False)

        keyname = 'cased'
        _id = Split.process(stream=keyname)
        doc = Split(_id)

        self.assertEqual(data_source[keyname].upper(), doc.uppercase.read())
        self.assertEqual(data_source[keyname].lower(), doc.lowercase.read())

        doc.uppercase.seek(0)
        doc.lowercase.seek(0)

        self.assertEqual('THIS IS A TEST.this is a test.', doc.cat.read())

    def test_can_read_computed_property_when_dependencies_are_in_different_data_stores(
            self):
        db1 = InMemoryDatabase(key_builder=self.Settings.key_builder)
        db2 = InMemoryDatabase(key_builder=self.Settings.key_builder)

        settings1 = self.Settings.clone(database=db1)
        settings2 = self.Settings.clone(database=db2)

        class Split(BaseModel, self.Settings):
            stream = Feature(TextStream, store=False)
            uppercase = Feature(
                ToUpper, needs=stream, store=True, persistence=settings1)
            lowercase = Feature(
                ToLower, needs=stream, store=True, persistence=settings2)
            cat = Feature(
                Concatenate, needs=[uppercase, lowercase], store=False)

        keyname = 'cased'
        _id = Split.process(stream=keyname)
        doc = Split(_id)

        _ids1 = set(db1.iter_ids())
        _ids2 = set(db2.iter_ids())
        self.assertTrue(_id in _ids1)
        self.assertTrue(_id in _ids2)

        self.assertEqual(data_source[keyname].upper(), doc.uppercase.read())
        self.assertEqual(data_source[keyname].lower(), doc.lowercase.read())

        self.assertEqual('THIS IS A TEST.this is a test.', doc.cat.read())

    def test_can_inherit(self):
        class A(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            uppercase = Feature(ToUpper, needs=stream, store=False)

        class B(A):
            lowercase = Feature(ToLower, needs=A.stream, store=False)

        _id = B.process(stream='cased')
        doc = B(_id)
        self.assertEqual(data_source['cased'].upper(), doc.uppercase.read())
        self.assertEqual(data_source['cased'].lower(), doc.lowercase.read())

    def test_can_change_key_builder_for_single_class(self):
        settings = self.Settings.clone(id_provider=IntegerIdProvider())

        class A(BaseModel, settings):
            stream = Feature(TextStream, store=True)
            uppercase = Feature(ToUpper, needs=stream, store=True)
            lowercase = Feature(ToLower, needs=stream, store=True)

        class B(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            uppercase = Feature(ToUpper, needs=stream, store=True)
            lowercase = Feature(ToLower, needs=stream, store=True)

        _id1 = A.process(stream='cased')
        self.assertEqual(1, _id1)
        _id2 = B.process(stream='cased')
        self.assertEqual(32, len(_id2))

    def test_can_read_different_documents_from_different_data_stores(self):
        db1 = InMemoryDatabase(key_builder=self.Settings.key_builder)
        db2 = InMemoryDatabase(key_builder=self.Settings.key_builder)

        settings1 = self.Settings.clone(database=db1)
        settings2 = self.Settings.clone(database=db2)

        class A(BaseModel, settings1):
            stream = Feature(TextStream, store=True)
            uppercase = Feature(ToUpper, needs=stream, store=True)
            lowercase = Feature(ToLower, needs=stream, store=False)

        class B(BaseModel, settings2):
            stream = Feature(TextStream, store=True)
            uppercase = Feature(ToUpper, needs=stream, store=True)
            lowercase = Feature(ToLower, needs=stream, store=False)

        _id1 = A.process(stream='mary')
        _id2 = B.process(stream='humpty')
        self.assertEqual(1, len(list(db1.iter_ids())))
        self.assertEqual(1, len(list(db2.iter_ids())))
        doc_a = A(_id1)
        doc_b = B(_id2)
        self.assertEqual(data_source['mary'].upper(), doc_a.uppercase.read())
        self.assertEqual(data_source['humpty'].upper(), doc_b.uppercase.read())

    def test_can_override_database_at_different_levels_of_granularity(self):
        db1 = InMemoryDatabase(key_builder=self.Settings.key_builder)
        db2 = InMemoryDatabase(key_builder=self.Settings.key_builder)

        settings1 = self.Settings.clone(database=db1)
        settings2 = self.Settings.clone(database=db2)

        class A(BaseModel, settings1):
            stream = Feature(TextStream, store=True)
            uppercase = Feature(ToUpper, needs=stream, store=True)
            lowercase = Feature(
                ToLower, needs=stream, store=True, persistence=settings2)

        _id = A.process(stream='cased')
        doc = A(_id)
        self.assertEqual(1, len(list(db1.iter_ids())))
        self.assertEqual(1, len(list(db2.iter_ids())))
        self.assertEqual(data_source['cased'].upper(), doc.uppercase.read())
        self.assertEqual(data_source['cased'].lower(), doc.lowercase.read())

    def test_can_register_multiple_dependencies_on_base_model_derived_class(
            self):
        db1 = InMemoryDatabase(key_builder=self.Settings.key_builder)

        settings = self.Settings.clone(
            database=db1, id_provider=IntegerIdProvider())

        class A(BaseModel, settings):
            stream = Feature(TextStream, store=True)
            uppercase = Feature(ToUpper, needs=stream, store=True)
            lowercase = Feature(ToLower, needs=stream, store=True)

        _id = A.process(stream='cased')
        doc = A(_id)
        self.assertEqual('1', list(db1.iter_ids())[0])
        self.assertEqual(data_source['cased'].upper(), doc.uppercase.read())
        self.assertEqual(data_source['cased'].lower(), doc.lowercase.read())

    def test_can_write_different_documents_to_different_data_stores(self):
        db1 = InMemoryDatabase(key_builder=self.Settings.key_builder)
        db2 = InMemoryDatabase(key_builder=self.Settings.key_builder)

        settings1 = self.Settings.clone(database=db1)

        class A(BaseModel, settings1):
            stream = Feature(TextStream, store=True)
            uppercase = Feature(ToUpper, needs=stream, store=True)
            lowercase = Feature(ToLower, needs=stream, store=False)

        settings2 = self.Settings.clone(database=db2)

        class B(BaseModel, settings2):
            stream = Feature(TextStream, store=True)
            uppercase = Feature(ToUpper, needs=stream, store=True)
            lowercase = Feature(ToLower, needs=stream, store=False)

        A.process(stream='mary')
        B.process(stream='humpty')
        self.assertEqual(1, len(list(db1.iter_ids())))
        self.assertEqual(1, len(list(db2.iter_ids())))

    def test_can_read_different_features_from_different_data_stores(self):
        db1 = InMemoryDatabase()
        db2 = InMemoryDatabase()

        settings1 = self.Settings.clone(database=db1)
        settings2 = self.Settings.clone(database=db2)

        class A(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            uppercase = Feature(
                ToUpper, needs=stream, store=True, persistence=settings1)
            lowercase = Feature(
                ToLower, needs=stream, store=True, persistence=settings2)

        _id = A.process(stream='mary')
        doc = A(_id)
        self.assertEqual(data_source['mary'].upper(), doc.uppercase.read())
        self.assertEqual(data_source['mary'].lower(), doc.lowercase.read())

    def test_can_write_different_features_to_different_data_stores(self):
        db1 = InMemoryDatabase(key_builder=self.Settings.key_builder)
        db2 = InMemoryDatabase(key_builder=self.Settings.key_builder)

        settings1 = self.Settings.clone(database=db1)
        settings2 = self.Settings.clone(database=db2)

        class A(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            uppercase = Feature(
                ToUpper, needs=stream, store=True, persistence=settings1)
            lowercase = Feature(
                ToLower, needs=stream, store=True, persistence=settings2)

        _id = A.process(stream='mary')
        _ids1 = set(db1.iter_ids())
        _ids2 = set(db2.iter_ids())

        self.assertTrue(_id in _ids1)
        self.assertTrue(_id in _ids2)

    def test_can_use_bz2_compression_encoder_and_decoder(self):
        class A(BaseModel, self.Settings):
            stream = Feature(TextStream, store=True)
            lowercase = CompressedFeature(ToLower, needs=stream, store=True)

        _id = A.process(stream='lorem')
        db = self.Settings.database
        key_builder = self.Settings.key_builder
        stream = db.read_stream(
            key_builder.build(_id, 'lowercase', A.lowercase.version))
        compressed = stream.read()
        self.assertTrue(len(compressed) < len(data_source['lorem']))
        doc = A(_id)
        self.assertEqual(data_source['lorem'].lower(), ''.join(doc.lowercase))


class InMemoryTest(BaseTest, unittest2.TestCase):
    def setUp(self):
        class Settings(PersistenceSettings):
            id_provider = UuidProvider()
            key_builder = StringDelimitedKeyBuilder()
            database = InMemoryDatabase(key_builder=key_builder)

        self.Settings = Settings


class FileSystemTest(BaseTest, unittest2.TestCase):
    def setUp(self):
        self._dir = mkdtemp()

        class Settings(PersistenceSettings):
            id_provider = UuidProvider()
            key_builder = StringDelimitedKeyBuilder()
            database = FileSystemDatabase(
                path=self._dir, key_builder=key_builder)

        self.Settings = Settings

    def tearDown(self):
        rmtree(self._dir)


class LmdbTest(BaseTest, unittest2.TestCase):
    def setUp(self):
        self._dir = mkdtemp()

        class Settings(PersistenceSettings):
            id_provider = UuidProvider()
            key_builder = StringDelimitedKeyBuilder()
            database = LmdbDatabase(
                path=self._dir,
                map_size=10000000,
                key_builder=key_builder)

        self.Settings = Settings

    def tearDown(self):
        rmtree(self._dir)
