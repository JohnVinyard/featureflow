from extractor import Node
from decoder import Decoder
from feature import Feature
from util import chunked
from requests import Session
import os
import struct
     
class ByteStream(Node):
    
    def __init__(self, chunksize = 4096, needs = None):
        super(ByteStream, self).__init__(needs = needs)
        self._chunksize = chunksize
    
    def _handle_http_request(self,data):
        s = Session()
        prepped = data.prepare()
        resp = s.send(prepped,stream = True)
        content_length = int(resp.headers['Content-Length'])
        for chunk in chunked(resp.raw, chunksize = self._chunksize):
            yield StringWithTotalLength(chunk,content_length)
    
    def _handle_local_file(self,data):
        try:
            with open(data,'rb') as f:
                content_length = int(os.path.getsize(data))
                for chunk in chunked(f, chunksize = self._chunksize):
                    yield StringWithTotalLength(chunk,content_length)
        except TypeError:
            content_length = data.seek(0, 2)
            data.seek(0)
            for chunk in chunked(data, chunksize = self._chunksize):
                yield StringWithTotalLength(chunk,content_length)
    
    def _process(self,data):
        try:
            for chunk in self._handle_http_request(data.uri):
                yield chunk
        except AttributeError:
            for chunk in self._handle_local_file(data.uri):
                yield chunk

class StringWithTotalLength(str):
    
    def __new__(cls,s,total_length):
        o = str.__new__(cls,s)
        o.total_length = int(total_length)
        return o
    
    def __radd__(self,other):
        return StringWithTotalLength(self + other, self.total_length)

class StringWithTotalLengthEncoder(Node):
    
    content_type = 'application/octet-stream'
    
    def __init__(self, needs = None):
        super(StringWithTotalLengthEncoder,self).__init__(needs = needs)
        self._metadata_written = False
    
    def _process(self, data):
        if not self._metadata_written:
            yield struct.pack('I',data.total_length)
            self._metadata_written
        yield data

class StringWithTotalLengthDecoder(Decoder):
    
    def __init__(self, chunksize = 4096):
        super(StringWithTotalLengthDecoder,self).__init__()
        self._chunksize = chunksize
        self._total_length = None
    
    def __call__(self,flo):
        return self.__iter__()
    
    def __iter__(self,flo):
        self._total_length = struct.unpack('I',flo.read(4))[0]
        for chunk in chunked(flo,self._chunksize):
            yield StringWithTotalLength(chunk, self._total_length)

class ByteStreamFeature(Feature):
    
    def __init__(\
         self,
         extractor,
         needs = None,
        store = False,
        key = None,
        **extractor_args):
        
        super(ByteStreamFeature,self).__init__(\
          extractor,
          needs = needs,
          store = store,
          encoder = StringWithTotalLengthEncoder,
          decoder = StringWithTotalLengthDecoder(\
             chunksize = extractor_args['chunksize']),
          key = key,
          **extractor_args)
                