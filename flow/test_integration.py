import unittest2
from StringIO import StringIO
from collections import defaultdict

from extractor import Extractor
from model import BaseModel
from feature import Feature,JSONFeature
from dependency_injection import Registry
from data import *

data_source = {
	'mary'   : 'mary had a little lamb little lamb little lamb',
	'humpty' : 'humpty dumpty sat on a wall humpty dumpty had a great fall' 
}

def chunked(f, chunksize = 4096):
    data = f.read(chunksize)
    while data:
        yield data
        data = f.read(chunksize)

class TextStream(Extractor):
    
    def __init__(self, needs = None):
        super(TextStream,self).__init__(needs = needs)
    
    def _process(self,final_push):
    	flo = StringIO(data_source[self._cache])
    	return chunked(flo,chunksize = 3)

class Tokenizer(Extractor):
    
    def __init__(self, needs = None):
        super(Tokenizer,self).__init__(needs = needs)

    def _update_cache(self,data,final_push,pusher):
	    '''
	    Update the data we're keeping stored until we can do some processing
	    '''
	    if not self._cache:
	    	self._cache = data
	    elif data:
	    	self._cache += data
    
    def _can_process(self,final_push):
    	if final_push:
    		return True
    	return self._cache.rfind(' ') != -1
    
    def _process(self,final_push):
        if not self._cache:
            return []

        if final_push:
        	return filter(lambda x : x, self._cache.split(' '))

        last_index = self._cache.rfind(' ')
        current = self._cache[:last_index + 1]
        self._cache = self._cache[last_index + 1:]
        return filter(lambda x : x, current.split(' '))

class WordCount(Extractor):
    
    def __init__(self, needs = None):
        super(WordCount,self).__init__(needs = needs)
        self._count = defaultdict(int)
    
    def _can_process(self,final_push):
        return final_push
    
    def _update_cache(self,data,final_push,pusher):
        for word in data:
            self._count[word.lower()] += 1
    
    def _process(self,final_push):
        return self._count

class Document(BaseModel):
    
    stream = Feature(TextStream, store = True)
    words  = Feature(Tokenizer, needs = stream, store = False)
    count  = JSONFeature(WordCount, needs = words, store = False)

class Document2(BaseModel):
    
    stream = Feature(TextStream, store = False)
    words  = Feature(Tokenizer, needs = stream, store = False)
    count  = JSONFeature(WordCount, needs = words, store = True)

class IntegrationTest(unittest2.TestCase):

	def setUp(self):
		Registry.register(IdProvider,UuidProvider())
		Registry.register(KeyBuilder,StringDelimitedKeyBuilder())
		Registry.register(Database,InMemoryDatabase())
		Registry.register(DataWriter,DataWriter)
		Registry.register(DataReader,DataReaderFactory())

	def test_can_process_and_retrieve_stored_feature(self):
		_id = Document.process('mary')
		doc = Document(_id)
		self.assertEqual(data_source['mary'],doc.stream.read())

	def test_can_correctly_decode_feature(self):
		_id = Document2.process('mary')
		doc = Document2(_id)
		self.assertTrue(isinstance(doc.count,dict))

	def test_can_retrieve_unstored_feature_when_dependencies_are_satisfied(self):
		_id = Document.process('humpty')
		doc = Document(_id)
		d = doc.count
		self.assertEqual(2,d['humpty'])
		self.assertEqual(1,d['sat'])

	def test_cannot_retrieve_unstored_feature_when_dependencies_are_not_satisfied(self):
		_id = Document2.process('humpty')
		doc = Document(_id)
		self.assertRaises(AttributeError,lambda : doc.stream)