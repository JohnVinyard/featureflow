import simplejson
import numpy as np

from encoder import IdentityEncoder,JSONEncoder,ShittyNumpyEncoder
from dependency_injection import dependency
from data import DataWriter

'''
- decoders shouldn't just be lambda functions, they should be classes
- How do I build decoders that allow me to decode in a streaming fashion, e.g.
  get only the pcm samples that describe seconds 10-15 of an audio file?
'''

class Feature(object):
    
    def __init__(\
            self,
            extractor,
            needs = None,
            store = False, 
            encoder = None, 
            decoder = None,
            key = None, 
            **extractor_args):
        
        super(Feature,self).__init__()
        self.key = key
        self.extractor = extractor
        self.store = store
        self.encoder = encoder or IdentityEncoder
        
        if needs is None:
            self.needs = []
        elif isinstance(needs,list):
            self.needs = needs
        else:
            self.needs = [needs]
        
        # decoding gets a little more complicated.  For some features (e.g.
        # metadata, I'd like to get access to a python dictionary when I call
        # the feature.  For others, (e.g. any time series data), I'd like to
        # get access to an object that lets me access time slices. 
        self.decoder = decoder or (lambda x : x)
        self.extractor_args = extractor_args   
    
    @property
    def is_root(self):
        return not self.needs      
    
    @property
    def content_type(self):
        return self.encoder.content_type
    
    @dependency(DataWriter)
    def _data_writer(self,needs = None, _id = None, feature_name = None):
        pass

    def _build_extractor(self,_id,extractors = None):
        if extractors[self.key]:
            return extractors[self.key]
        
        needs = [extractors[f.key] or f._build_extractor(_id,extractors) \
             for f in self.needs]
        e = self.extractor(needs = needs,**self.extractor_args)
        extractors[self.key] = e
        
        if not self.store:
            return e

        encoder = self.encoder(needs = e)
        # TODO: Here the DataWriter is monolithic.  What if the data writer 
        # varies by feature, e.g., some values are written to a database, while
        # others are published to a work queue?
        self._data_writer(\
            needs = encoder, _id = _id, feature_name = self.key)        
        
        return e

class JSONFeature(Feature):
    
    def __init__(self,extractor,needs = None,store = False,key = None,**extractor_args):
        super(JSONFeature,self).__init__(\
            extractor,
            needs = needs,
            store = store,
            encoder = JSONEncoder,
            decoder = lambda x : simplejson.loads(x.read()),
            key = key,
            **extractor_args)

class NumpyFeature(Feature):
    
    def __init__(self,extractor,needs = None,store = False,key = None,**extractor_args):
        super(NumpyFeature,self).__init__(\
            extractor,
            needs = needs,
            store = store,
            encoder = ShittyNumpyEncoder,
            decoder = lambda x : np.fromstring(x.read(),dtype = np.float32),
            key = key,
            **extractor_args)

