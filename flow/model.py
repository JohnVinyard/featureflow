from dependency_injection import dependency
from feature import Feature
from data import DataReader,IdProvider

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
        if isinstance(f,Feature):
            feature = getattr(self.__class__,key)
            raw = self.reader(self._id, key)
            decoder = feature.decoder(raw)
            setattr(self,key,decoder)
            return decoder    
        else:   
            return f
    
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
    @dependency(IdProvider)
    def id_provider(cls):
        pass 
     
    @classmethod
    def process(cls,inp):
        _id = cls.id_provider().new_id()
        extractor = cls._build_extractor(_id)
        extractor.process(inp)
        return _id