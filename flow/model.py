from dependency_injection import dependency
from feature import Feature
from data import DataReader,IdProvider,StringIODataWriter
from decoder import DecoderExtractor
from collections import deque

class MetaModel(type):

    def __init__(self,name,bases,attrs):
        
        self.features = {}
        
        for b in bases:
            self._add_features(b.__dict__)
        
        self._add_features(attrs)
        
        super(MetaModel,self).__init__(name,bases,attrs)
            
    def _add_features(self,d):
        for k,v in d.iteritems():
            if isinstance(v,Feature):
                v.key = k
                self.features[k] = v
    
    def iterfeatures(self):
        return self.features.iteritems()
    
    def stored_features(self):
        return filter(lambda f : f.store,self.features.itervalues())
    

class BaseModel(object):
    
    __metaclass__ = MetaModel
    
    def __init__(self,_id):
        super(BaseModel,self).__init__()
        self._id = _id
    
    @dependency(DataReader)
    def reader(self,_id,key):
        pass
    
    def __getattribute__(self,key):
        f = object.__getattribute__(self,key)

        if not isinstance(f,Feature): 
            return f

        feature = getattr(self.__class__,key)

        if f.store:        
            raw = self.reader(self._id, key)
            decoded = feature.decoder(raw)
            setattr(self,key,decoded)
            return decoded

        if not f._can_compute():
            raise AttributeError('%s cannot be computed' % f.key)

        partial,data_writer = self._build_partial(self._id,f)
        # TODO: Zip/interleave the roots
        for p in partial:
            p.process(self.reader(self._id,p.key))

        stream = data_writer._stream
        stream.seek(0)
        decoded = feature.decoder(stream)
        setattr(self,key,decoded)
        return decoded
    
    @classmethod
    def _build_extractor(cls,_id):
        extractors = dict([(k,None) for k in cls.features.iterkeys()])
        root = None
        for feature in cls.features.itervalues():
            e = feature._build_extractor(_id,extractors)
            if feature.is_root:
                if root:
                    raise ValueError('multiple root extractors')
                root = e
        
        if not root:
            raise ValueError('no root extractor')
        return root

    @classmethod
    def _build_partial(cls,_id,feature):
        features = feature._partial(_id)
        extractors = dict([(k,None) for k in features.iterkeys()])
        roots = []
        for feat in features.itervalues():
            e = feat._build_extractor(_id,extractors)
            if feat.is_root: 
                roots.append(Partial(feat,e))
            if feat.key == feature.key:
                data_writer = e.find_listener(\
                    lambda x : isinstance(x,StringIODataWriter))
        return roots,data_writer
    
    @classmethod
    @dependency(IdProvider)
    def id_provider(cls): pass 
     
    @classmethod
    def process(cls,inp):
        _id = cls.id_provider().new_id()
        extractor = cls._build_extractor(_id)
        extractor.process(inp)
        return _id

class Partial(object):

    def __init__(self,feature,extractor):
        super(Partial,self).__init__()
        self.feature = feature
        self.extractor = extractor

    def process(self,data):
        return self.extractor.process(data)

    @property
    def key(self):
        return self.feature.key
