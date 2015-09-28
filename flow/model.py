from extractor import Graph
from dependency_injection import dependency
from feature import Feature
from data import IdProvider,DataWriter,StringIODataWriter

class MetaModel(type):
    
    def __init__(self,name,bases,attrs):
        
        self.features = {}
        
        for b in bases:
            self._add_features(b.__dict__)
        
        self._add_features(attrs)
        
        super(MetaModel,self).__init__(name,bases,attrs)
    
    def iter_features(self):
        return self.features.itervalues()

    # KLUDGE: I shouldn't know about the DI code here, but
    # this is my best idea about how to ensure that 
    # model-level registries can be passed along to features.
    # It's important to understand that the decorator code doesn't
    # run until after __init__, which is too late.
    def _on_register(self):
        for v in self.features.itervalues():
            try:
                v.set_registry(self._registry)
            except AttributeError:
                pass
            
    def _add_features(self,d):
        for k,v in d.iteritems():
            if not isinstance(v,Feature): continue
            v.key = k
            self.features[k] = v
            
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

        try:
            raw = f.reader(self._id, key)
            decoded = feature.decoder(raw)
            setattr(self,key,decoded)
            return decoded
        except KeyError:
            pass
        
        if not f._can_compute():
            raise AttributeError('%s cannot be computed' % f.key)

        graph, stream = self._build_partial(self._id, f)
        
        kwargs = dict()
        for k,extractor in graph.roots().iteritems():
            try:
                kwargs[k] = extractor._reader
            except AttributeError:
                kwargs[k] = f.reader(self._id, k)
          
        graph.process(**kwargs)
        if stream is None:
            stream = feature.reader(self._id, feature.key)
        stream.seek(0)
        decoded = feature.decoder(stream)
        setattr(self,key,decoded)
        return decoded
    
    @classmethod
    def _build_extractor(cls,_id):
        g = Graph()
        for feature in cls.features.itervalues():
            feature._build_extractor(_id, g)
            
            
        return g

    @classmethod
    def _build_partial(cls, _id, feature):
        features = feature._partial(_id)
        g = Graph()
        for feat in features.itervalues():
            e = feat._build_extractor(_id, g)
            if feat.key == feature.key:
                stream = e.find_listener(\
                  lambda x : isinstance(x, StringIODataWriter))
                if stream is not None:
                    stream = stream._stream
                
        return g, stream
    
    @classmethod
    @dependency(IdProvider)
    def id_provider(cls): pass
    
    
     
    @classmethod
    def process(cls, **kwargs):
        _id = cls.id_provider().new_id(**kwargs)
        graph = cls._build_extractor(_id)
        graph.remove_dead_nodes(cls.features.itervalues())
        graph.process(**kwargs)
        return _id