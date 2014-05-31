'''
decoders should be a callables that take a file-like object and return...anything
'''
import simplejson

class Decoder(object):
	'''
	The simplest possible decoder takes a file-like object and returns it
	'''
	def __init__(self):
		super(Decoder,self).__init__()

	def __call__(self,flo):
		return flo

class GreedyDecoder(Decoder):
	'''
	A decoder that reads the entire file contents into memory
	'''
	def __init__(self):
		super(GreedyDecoder,self).__init__()

	def __call__(self,flo):
		return flo.read()

class JSONDecoder(GreedyDecoder):
	'''
	A decoder that interprets the data as JSON
	'''
	def __init__(self):
		super(JSONDecoder,self).__init__()

	def __call__(self,flo):
		return simplejson.loads(super(JSONDecoder,self)(flo))

