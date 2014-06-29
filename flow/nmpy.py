import numpy as np
from extractor import Node
from decoder import Decoder
from feature import Feature
import struct

class NumpyMetaData(object):

	def __init__(self,dtype = None,shape = None):
		self.dtype = np.uint8 if dtype is None else dtype
		self.shape = shape or ()

	def __repr__(self):
		return repr((str(np.dtype(self.dtype)),self.shape))

	def __str__(self):
		return self.__repr__()

	def pack(self):
		s = str(self)
		l = len(s)
		return struct.pack('B{n}s'.format(n = l),l,s)

	@classmethod
	def unpack(cls,flo):
		l = struct.unpack('B',flo.read(1))[0]
		bytes_read = 1 + l
		return cls(*eval(flo.read(l))),bytes_read

class NumpyEncoder(Node):
    
    content_type = 'application/octet-stream'
    
    def __init__(self, needs = None):
        super(NumpyEncoder,self).__init__(needs = needs)
        self.metadata = None
    
    def _process(self,data):
    	if not self.metadata:
    		self.metadata = NumpyMetaData(\
    			dtype = data.dtype, shape = data.shape[1:])
    		yield self.metadata.pack()

        yield data.tostring()

class GreedyNumpyDecoder(Node):

	def __init__(self,needs = None):
		super(GreedyNumpyDecoder,self).__init__(needs = needs)

	def __call__(self,flo):
		metadata,bytes_read = NumpyMetaData.unpack(flo)
		leftovers = flo.read()
		leftover_bytes = len(leftovers)
		itemsize = np.dtype(metadata.dtype).itemsize
		prod = np.product(metadata.shape)
		first_dim = (leftover_bytes / (prod * itemsize))
		dim = (first_dim,) + metadata.shape
		f = np.frombuffer if len(leftovers) else np.fromstring
		return f(leftovers,dtype = metadata.dtype).reshape(dim)

	def __iter__(self,flo):
		yield self(flo)

class NumpyFeature(Feature):
    
    def __init__(self,extractor,needs = None,store = False,key = None,**extractor_args):
        super(NumpyFeature,self).__init__(\
            extractor,
            needs = needs,
            store = store,
            encoder = NumpyEncoder,
            decoder = GreedyNumpyDecoder(),
            key = key,
            **extractor_args)
