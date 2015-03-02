import unittest2
from collections import defaultdict
import random

from extractor import NotEnoughData,Aggregator
from model import BaseModel
from feature import Feature,JSONFeature
from dependency_injection import Registry,register
from data import *
from util import chunked,TempDir

data_source = {
	'mary'   : 'mary had a little lamb little lamb little lamb',
	'humpty' : 'humpty dumpty sat on a wall humpty dumpty had a great fall',
	'numbers' : range(10),
	'cased' : 'This is a test.'
}

class TextStream(Node):
	
	def __init__(self, chunksize = 3, needs = None):
		super(TextStream,self).__init__(needs = needs)
		self._chunksize = chunksize
	
	def _process(self,data):
		try:
			flo = StringIO(data_source[data])
		except KeyError:
			flo = StringIO(data)
		
		for chunk in chunked(flo,chunksize = self._chunksize):
			yield chunk

class ToUpper(Node):

	def __init__(self, needs = None):
		super(ToUpper,self).__init__(needs = needs)

	def _process(self,data):
		yield data.upper()

class ToLower(Node):

	def __init__(self, needs = None):
		super(ToLower,self).__init__(needs = needs)

	def _process(self,data):
		yield data.lower()

class Concatenate(Aggregator,Node):

	def __init__(self, needs = None):
		super(Concatenate,self).__init__(needs = needs)
		self._cache = defaultdict(str)

	def _enqueue(self,data,pusher):
		self._cache[id(pusher)] += data

	def _process(self,data):
		yield ''.join(data.itervalues())

class WordCountAggregator(Aggregator,Node):
	
	def __init__(self, needs = None):
		super(WordCountAggregator,self).__init__(needs = needs)
		self._cache = defaultdict(int)
	
	def _enqueue(self,data,pusher):
		for k,v in data.iteritems():
			self._cache[k.lower()] += v

class SumUp(Node):

	def __init__(self, needs = None):
		super(SumUp,self).__init__(needs = needs)
		self._cache = dict()

	def _enqueue(self,data,pusher):
		self._cache[id(pusher)] = data

	def _dequeue(self):
		if len(self._cache) != len(self._needs):
			raise NotEnoughData()
		v = self._cache
		self._cache = dict()
		return v

	def _process(self,data):
		results = [str(sum(x)) for x in zip(*data.itervalues())]
		yield ''.join(results)

class EagerConcatenate(Node):
	
	def __init__(self, needs = None):
		super(EagerConcatenate,self).__init__(needs = needs)
		self._cache = dict()
	
	def _enqueue(self,data,pusher):
		self._cache[id(pusher)] = data
	
	def _dequeue(self):
		v, self._cache = self._cache, dict()
		return v
	
	def _process(self,data):
		yield ''.join(data.itervalues())

class NumberStream(Node):

	def __init__(self, needs = None):
		super(NumberStream,self).__init__(needs = needs)

	def _process(self,data):
		l = data_source[data]
		for i in xrange(0,len(l),3):
			yield l[i : i + 3]

class Add(Node):

	def __init__(self, rhs = 1, needs = None):
		super(Add,self).__init__(needs = needs)
		self._rhs = rhs

	def _process(self,data):
		yield [c + self._rhs for c in data]


class Tokenizer(Node):
	
	def __init__(self, needs = None):
		super(Tokenizer,self).__init__(needs = needs)
		self._cache = ''

	def _enqueue(self,data,pusher):
		self._cache += data

	def _finalize(self,pusher):
		self._cache += ' '

	def _dequeue(self):
		last_index = self._cache.rfind(' ')
		if last_index == -1:
			raise NotEnoughData()
		current = self._cache[:last_index + 1]
		self._cache = self._cache[last_index + 1:]
		return current
	
	def _process(self,data):
		yield filter(lambda x : x, data.split(' '))

class WordCount(Aggregator,Node):
	
	def __init__(self, needs = None):
		super(WordCount,self).__init__(needs = needs)
		self._cache = defaultdict(int)

	def _enqueue(self,data,pusher):
		for word in data:
			self._cache[word.lower()] += 1

class FeatureAggregator(Node):
	
	def __init__(self, cls = None, feature = None, needs = None):
		super(FeatureAggregator,self).__init__(needs = needs)
		self._cls = cls
		self._feature = feature
	
	def _process(self,data):
		db = data
		for key in db.iter_ids():
			try:
				doc = self._cls(key)
				yield getattr(doc,self._feature.key)
			except:
				pass

# TODO: Move these classes into the tests, where possible
class Document(BaseModel):
	
	stream = Feature(TextStream, store = True)
	words  = Feature(Tokenizer, needs = stream, store = False)
	count  = JSONFeature(WordCount, needs = words, store = False)

class DocumentWordCount(BaseModel):
	counts = Feature(\
		FeatureAggregator, 
		cls = Document, 
		feature = Document.count,
		store = False)
	
	total_count = JSONFeature(\
		WordCountAggregator, 
		store = True, 
		needs = counts)

class Document2(BaseModel):
	
	stream = Feature(TextStream, store = False)
	words  = Feature(Tokenizer, needs = stream, store = False)
	count  = JSONFeature(WordCount, needs = words, store = True)

class Doc4(BaseModel):
	
	stream = Feature(TextStream, chunksize = 10, store = False)
	smaller = Feature(TextStream, needs = stream, chunksize = 3, store = True)

class MultipleRoots(BaseModel):
	
	stream1 = Feature(TextStream, chunksize = 3, store = False)
	stream2 = Feature(TextStream, chunksize = 3, store = False)
	cat = Feature(EagerConcatenate, needs = [stream1,stream2], store = True)

class BaseTest(object):
	
	def test_can_aggregate_word_counts_from_multiple_inputs(self):
		class Contrived(BaseModel):
			stream1 = Feature(TextStream,store = False)
			stream2 = Feature(TextStream,store = False)
			t1 = Feature(Tokenizer,needs = stream1,store = False)
			t2 = Feature(Tokenizer,needs = stream2,store = False)
			count1 = JSONFeature(WordCount, needs = t1, store = True)
			count2 = JSONFeature(WordCount, needs = t2, store = True)
			aggregate = JSONFeature(\
					WordCountAggregator, needs = [count1,count2], store = True)
		
		_id = Contrived.process(stream1 = 'mary',stream2 = 'humpty')
		doc = Contrived(_id)
		self.assertEqual(3,doc.aggregate['a'])

	def test_stored_features_are_not_rewritten_when_computing_dependent_feature(self):	
		class Timestamp(Aggregator,Node):
	
			def __init__(self, needs = None):
				super(Timestamp,self).__init__(needs = needs)
				self._cache = ''
			
			def _enqueue(self,data,pusher):
				self._cache += data
			
			def _process(self,data):
				yield str(random.random())

		class Timestamps(BaseModel):
			stream = Feature(TextStream, store = True)
			t1 = Feature(Timestamp, needs = stream, store = True)
			t2 = Feature(Timestamp, needs = stream, store = False)
			cat = Feature(\
				Concatenate, needs = [t1,t2], store = False)
		
		_id = Timestamps.process(stream = 'cased')
		doc1 = Timestamps(_id)
		orig = doc1.t1.read()
		doc2 = Timestamps(_id)
		_ = doc2.cat.read()
		new = doc2.t1.read()
		self.assertEqual(orig,new)
		self.assertEqual(doc2.t1.read() + doc2.t1.read(),doc2.cat.read())
	
	def test_can_process_multiple_documents_and_then_aggregate_word_count(self):
		_id1 = Document.process(stream = 'mary')
		_id2 = Document.process(stream = 'humpty')
		_id3 = DocumentWordCount.process(\
			counts = Registry.get_instance(Database))
		doc = DocumentWordCount(_id3)
		self.assertEqual(3,doc.total_count['a'])
	
	def test_document_with_multiple_roots(self):
		_id = MultipleRoots.process(stream1 = 'mary', stream2 = 'humpty')
		doc = MultipleRoots(_id)
		data = doc.cat.read()
		# KLUDGE: Note that order is unknown, since we're using a dict 
		self.assertTrue('mar' in data[:6])
		self.assertTrue('hum' in data[:6])
		self.assertEqual(\
			len(data_source['mary']) + len(data_source['humpty']), 
			len(data))
		self.assertTrue(data.endswith('fall'))
	
	def test_smaller_chunks_downstream(self):
		_id = Doc4.process(stream = 'mary')
		doc = Doc4(_id)
		self.assertEqual(data_source['mary'],doc.smaller.read())
	
	def test_exception_is_thrown_if_all_kwargs_are_not_provided(self):
		self.assertRaises(\
			KeyError, 
			lambda : MultipleRoots.process(stream1 = 'mary'))

	def test_can_process_and_retrieve_stored_feature(self):
		_id = Document.process(stream = 'mary')
		doc = Document(_id)
		self.assertEqual(data_source['mary'],doc.stream.read())

	def test_can_correctly_decode_feature(self):
		_id = Document2.process(stream = 'mary')
		doc = Document2(_id)
		self.assertTrue(isinstance(doc.count,dict))

	def test_can_retrieve_unstored_feature_when_dependencies_are_satisfied(self):
		_id = Document.process(stream = 'humpty')
		doc = Document(_id)
		d = doc.count
		self.assertEqual(2,d['humpty'])
		self.assertEqual(1,d['sat'])

	def test_cannot_retrieve_unstored_feature_when_dependencies_are_not_satisfied(self):
		_id = Document2.process(stream = 'humpty')
		doc = Document2(_id)
		self.assertRaises(AttributeError,lambda : doc.stream)

	def test_feature_with_multiple_inputs(self):
		
		class Numbers(BaseModel):
			stream = Feature(NumberStream,store = False)
			add1 = Feature(Add, needs = stream, store = False, rhs = 1)
			add2 = Feature(Add, needs = stream, store = False, rhs = 1)
			sumup = Feature(SumUp, needs = [add1,add2], store = True)

		_id = Numbers.process(stream = 'numbers')
		doc = Numbers(_id)
		self.assertEqual('2468101214161820',doc.sumup.read())

	def test_unstored_feature_with_multiple_inputs_can_be_computed(self):
		
		class Doc3(BaseModel):
			stream = Feature(TextStream, store = True)
			uppercase = Feature(ToUpper, needs = stream, store = True)
			lowercase = Feature(ToLower, needs = stream, store = False)
			cat = Feature(\
				Concatenate, needs = [uppercase,lowercase], store = False)

		_id = Doc3.process(stream = 'cased')
		doc = Doc3(_id)
		self.assertEqual('this is a test.THIS IS A TEST.',doc.cat.read())
	
	def test_can_read_computed_property_with_multiple_dependencies(self):
		
		class Split(BaseModel):
			stream = Feature(TextStream, store = False)
			uppercase = Feature(ToUpper, needs = stream, store = True)
			lowercase = Feature(ToLower, needs = stream, store = True)
			cat = Feature(\
				Concatenate, needs = [uppercase,lowercase], store = False)
		
		keyname = 'cased'
		_id = Split.process(stream = keyname)
		doc = Split(_id)
		
		self.assertEqual(data_source[keyname].upper(),doc.uppercase.read())
		self.assertEqual(data_source[keyname].lower(),doc.lowercase.read())
		
		doc.uppercase.seek(0)
		doc.lowercase.seek(0)
		
		self.assertEqual('this is a test.THIS IS A TEST.',doc.cat.read())
	
	def test_can_read_computed_property_when_dependencies_are_in_different_data_stores(self):
		db1 = InMemoryDatabase('alt1')
		db2 = InMemoryDatabase('alt2')
		
		class Split(BaseModel):
			stream = Feature(TextStream, store = False)
			uppercase = Feature(ToUpper, needs = stream, store = True)
			lowercase = Feature(ToLower, needs = stream, store = True)
			cat = Feature(\
				Concatenate, needs = [uppercase,lowercase], store = False)
			
			register(uppercase,Database,db1)
			register(lowercase,Database,db2)

		
		keyname = 'cased'
		_id = Split.process(stream = keyname)
		doc = Split(_id)
		
		_ids1 = set(db1.iter_ids())
		_ids2 = set(db2.iter_ids())
		self.assertTrue(_id in _ids1)
		self.assertTrue(_id in _ids2)
		
		self.assertEqual(data_source[keyname].upper(),doc.uppercase.read())
		self.assertEqual(data_source[keyname].lower(),doc.lowercase.read())
		
		self.assertEqual('this is a test.THIS IS A TEST.',doc.cat.read())
	
	
	def test_can_read_different_documents_from_different_data_stores(self):
		db1 = InMemoryDatabase()
		db2 = InMemoryDatabase()
		
		@register(Database,db1)
		class A(BaseModel):
			stream = Feature(TextStream, store = True)
			uppercase = Feature(ToUpper, needs = stream, store = True)
			lowercase = Feature(ToLower, needs = stream, store = False)
			
		
		@register(Database,db2)
		class B(BaseModel):
			stream = Feature(TextStream, store = True)
			uppercase = Feature(ToUpper, needs = stream, store = True)
			lowercase = Feature(ToLower, needs = stream, store = False)
			
		
		_id1 = A.process(stream = 'mary')
		_id2 = B.process(stream = 'humpty')
		self.assertEqual(1,len(list(db1.iter_ids())))
		self.assertEqual(1,len(list(db2.iter_ids())))
		doc_a = A(_id1)
		doc_b = B(_id2)
		self.assertEqual(data_source['mary'].upper(),doc_a.uppercase.read())
		self.assertEqual(data_source['humpty'].upper(),doc_b.uppercase.read())
	
	def test_can_write_different_documents_to_different_data_stores(self):
		db1 = InMemoryDatabase()
		db2 = InMemoryDatabase()
		
		@register(Database,db1)
		class A(BaseModel):
			stream = Feature(TextStream, store = True)
			uppercase = Feature(ToUpper, needs = stream, store = True)
			lowercase = Feature(ToLower, needs = stream, store = False)
			
		
		@register(Database,db2)
		class B(BaseModel):
			stream = Feature(TextStream, store = True)
			uppercase = Feature(ToUpper, needs = stream, store = True)
			lowercase = Feature(ToLower, needs = stream, store = False)
			

		_id1 = A.process(stream = 'mary')
		_id2 = B.process(stream = 'humpty')
		self.assertEqual(1,len(list(db1.iter_ids())))
		self.assertEqual(1,len(list(db2.iter_ids())))
	
	def test_can_read_different_features_from_different_data_stores(self):
		db1 = InMemoryDatabase()
		db2 = InMemoryDatabase()
		
		class A(BaseModel):
			stream = Feature(TextStream, store = True)
			uppercase = Feature(ToUpper, needs = stream, store = True)
			lowercase = Feature(ToLower, needs = stream, store = True)
			
			register(uppercase,Database,db1)
			register(lowercase,Database,db2)
				
		_id = A.process(stream = 'mary')
		doc = A(_id)
		self.assertEqual(data_source['mary'].upper(),doc.uppercase.read())
		self.assertEqual(data_source['mary'].lower(),doc.lowercase.read())
	
	def test_can_write_different_features_to_different_data_stores(self):
		db1 = InMemoryDatabase()
		db2 = InMemoryDatabase()
		
		class A(BaseModel):
			stream = Feature(TextStream, store = True)
			uppercase = Feature(ToUpper, needs = stream, store = True)
			lowercase = Feature(ToLower, needs = stream, store = True)
			
			register(uppercase,Database,db1)
			register(lowercase,Database,db2)
				
		_id = A.process(stream = 'mary')
		_ids1 = set(db1.iter_ids())
		_ids2 = set(db2.iter_ids())
		
		self.assertTrue(_id in _ids1)
		self.assertTrue(_id in _ids2)

class InMemoryTest(BaseTest,unittest2.TestCase):

	def setUp(self):
		Registry.register(IdProvider,UuidProvider())
		Registry.register(KeyBuilder,StringDelimitedKeyBuilder())
		Registry.register(Database,InMemoryDatabase(name = 'root'))
		Registry.register(DataWriter,DataWriter)

class FileSystemTest(BaseTest,unittest2.TestCase):
	
	def setUp(self):
		self._dir = TempDir()
		Registry.register(IdProvider,UuidProvider())
		Registry.register(KeyBuilder,StringDelimitedKeyBuilder())
		Registry.register(Database,FileSystemDatabase(path = self._dir.path))
		Registry.register(DataWriter,DataWriter)
	
	def tearDown(self):
		self._dir.cleanup()
	
		
