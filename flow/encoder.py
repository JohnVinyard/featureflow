import simplejson
from extractor import Node,NotEnoughData
import bz2

class IdentityEncoder(Node):
    
    content_type = 'application/octet-stream'
    
    def __init__(self, needs = None):
        super(IdentityEncoder,self).__init__(needs = needs)

    def _enqueue(self,data,pusher):
        self._cache = data if data else ''

class TextEncoder(IdentityEncoder):
    
    content_type = 'text/plain'
    
    def __init__(self,needs = None):
        super(TextEncoder,self).__init__(needs = needs)

class JSONEncoder(Node):
    
    content_type = 'application/json'
    
    def __init__(self, needs = None):
        super(JSONEncoder,self).__init__(needs = needs)

    def dequeue(self):
        if not self._finalized:
            raise NotEnoughData()

        return super(JSONEncoder,self)._dequeue()
        
    def _process(self,data):
        yield simplejson.dumps(data)

class BZ2Encoder(Node):
    
    content_type = 'application/octet-stream'
    
    def __init__(self, needs = None):
        super(BZ2Encoder,self).__init__(needs = needs)
        self._compressor = None
    
    def _finalize(self,pusher):
        self._cache = ''
    
    def _process(self,data):
        if self._compressor is None:
            self._compressor = bz2.BZ2Compressor()
        compressed = self._compressor.compress(data)
        if compressed: yield compressed
        if self._finalized: 
            yield self._compressor.flush()
        

