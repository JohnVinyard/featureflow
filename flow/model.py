from extractor import Graph
from dependency_injection import dependency
from feature import Feature
from data import IdProvider,StringIODataWriter

class MetaModel(type):

    def __init__(self,name,bases,attrs):
        
        self.features = {}
        
        for b in bases:
            self._add_features(b.__dict__)
        
        self._add_features(attrs)
        
        super(MetaModel,self).__init__(name,bases,attrs)
            
    def _add_features(self,d):
        for k,v in d.iteritems():
            if not isinstance(v,Feature): continue
            v.key = k
            self.features[k] = v
            
            # KLUDGE: I shouldn't know about the DI code here
            try:
                v.set_registry(self._registry)
            except AttributeError:
                pass
    
#    def iterfeatures(self):
#        return self.features.iteritems()
    
#    def stored_features(self):
#        return filter(lambda f : f.store,self.features.itervalues())
    

class BaseModel(object):
    
    __metaclass__ = MetaModel
    
    def __init__(self,_id):
        super(BaseModel,self).__init__()
        self._id = _id
        
    def __getattribute__(self,key):
        f = object.__getattribute__(self,key)

        if not isinstance(f,Feature): 
            return f

        feature = getattr(self.__class__,key)

        if f.store:        
            raw = f.reader(self._id, key)
            decoded = feature.decoder(raw)
            setattr(self,key,decoded)
            return decoded

        if not f._can_compute():
            raise AttributeError('%s cannot be computed' % f.key)

        graph,data_writer = self._build_partial(self._id,f)
        
        # BUG: The issue here is that I'm assuming the reader for f should be
        # used for all nodes in the extractor graph, which is not accurate.
#        kwargs = dict(\
#          (k,f.reader(self._id,k)) for k,_ in graph.roots().iteritems())
        
#        kwargs = dict(\
#          (k,e.__reader or self.reader(self._id,k)) for k,e in graph.roots().iteritems())
#        print kwargs
        kwargs = dict()
        for k,extractor in graph.roots().iteritems():
            try:
                reader = extractor.__reader
            except AttributeError:
                reader = f.reader(self._id,k)
            kwargs[k] = reader  
        graph.process(**kwargs)

        stream = data_writer._stream
        stream.seek(0)
        decoded = feature.decoder(stream)
        setattr(self,key,decoded)
        return decoded
    
    @classmethod
    def _build_extractor(cls,_id):
        g = Graph()
        for feature in cls.features.itervalues():
            feature._build_extractor(_id,g)
        return g

    @classmethod
    def _build_partial(cls,_id,feature):
        features = feature._partial(_id)
        g = Graph()
        for feat in features.itervalues():
            e = feat._build_extractor(_id,g)
            if feat.key == feature.key:
                data_writer = e.find_listener(\
                    lambda x : isinstance(x,StringIODataWriter))
        return g,data_writer
    
    @classmethod
    @dependency(IdProvider)
    def id_provider(cls): pass 
     
    @classmethod
    def process(cls,**kwargs):
        _id = cls.id_provider().new_id()
        graph = cls._build_extractor(_id)
        graph.process(**kwargs)
        return _id