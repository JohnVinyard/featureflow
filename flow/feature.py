from encoder import IdentityEncoder,JSONEncoder,TextEncoder,BZ2Encoder
from decoder import JSONDecoder,Decoder,GreedyDecoder,DecoderNode,BZ2Decoder
from dependency_injection import dependency
from data import DataWriter,StringIODataWriter,Database,KeyBuilder

class Feature(object):
    
    def __init__(\
            self,
            extractor,
            needs = None,
            store = False, 
            encoder = None, 
            decoder = None,
            key = None,
            data_writer = None,
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
        
        self.decoder = decoder or Decoder()
        self.extractor_args = extractor_args

        if data_writer:
            self._data_writer = data_writer
            
    def __repr__(self):
        return '{cls}(key = {key}, store = {store})'.format(\
            cls = self.__class__.__name__,**self.__dict__)
    
    def __str__(self):
        return self.__repr__()
    
    def copy(\
        self,
        extractor = None,
        needs = None,
        store = None,
        data_writer = None,
        extractor_args = None):
        '''
        Use self as a template to build a new feature, replacing
        values in kwargs
        '''
        f = Feature(\
            extractor or self.extractor,
            needs = needs,
            store = self.store if store is None else store,
            encoder = self.encoder,
            decoder = self.decoder,
            key = self.key,
            data_writer = data_writer,
            **(extractor_args or self.extractor_args))
        try:
            f.set_registry(self._registry)
        except AttributeError:
            pass
        
        return f
    
    def set_registry(self,registry):
        if not hasattr(self,'_registry'):
            self._registry = registry

    def add_dependency(self,feature):
        self.needs.append(feature)
    
    @property
    @dependency(KeyBuilder)
    def keybuilder(self): pass

    @property
    @dependency(Database)
    def database(self): pass
    
    @dependency(DataWriter)
    def _data_writer(self,needs = None, _id = None, feature_name = None):
        pass
    
    def reader(self,_id,key):
        return self.database.read_stream(\
             self.keybuilder.build(_id,key))

    @property
    def is_root(self):
        return not self.needs      
    
    @property
    def content_type(self):
        return self.encoder.content_type
    
    def _can_compute(self):
        '''
        Return true if this feature stored, or is unstored, but can be computed
        from stored dependencies
        '''
        if self.store:
            return True

        if self.is_root:
            return False

        return all([n._can_compute() for n in self.needs])

    def _partial(self,_id,features = None):
        '''
        TODO: _partial is a shit name for this, kind of.  I'm building a graph
        such that I can only do work necessary to compute self, and no more
        '''
        if self.store and features is None:
            raise Exception('There is no need to build a partial graph for a stored feature')

        nf = self.copy(\
            extractor = DecoderNode if self.store else self.extractor,
            store = features is None,
            needs = None,
            data_writer = StringIODataWriter if features is None else None,
            extractor_args = dict(decodifier = self.decoder) \
                if self.store else self.extractor_args)

        if features is None:
            features = dict()

        features[self.key] = nf

        if not self.store:
            for n in self.needs:
                n._partial(_id,features = features)
                nf.add_dependency(features[n.key])

        return features

    def _depends_on(self,_id,graph):
        needs = []
        for f in self.needs:
            if f.key in graph:
                needs.append(graph[f.key])
                continue
            e = f._build_extractor(_id,graph)
            needs.append(e)
        return needs

    def _build_extractor(self,_id,graph):
        try:
            return graph[self.key]
        except KeyError:
            pass
        
        needs = self._depends_on(_id,graph)
        e = self.extractor(needs = needs,**self.extractor_args)
        if isinstance(e,DecoderNode):
            reader = self.reader(_id,self.key)
            setattr(e,'_reader',reader)
            
        graph[self.key] = e
        if not self.store: return e
        key = self.key
        encoder = self.encoder(needs = e)
        graph['{key}_encoder'.format(**locals())] = encoder
        
        dw = self._data_writer(\
            needs = encoder, _id = _id, feature_name = self.key)
        
        graph['{key}_writer'.format(**locals())] = dw
        return e

class CompressedFeature(Feature):
    
    def __init__(\
            self,
            extractor,
            needs = None,
            store = False,
            key = None,
            **extractor_args):
        
        super(CompressedFeature,self).__init__(\
            extractor,
            needs = needs,
            store = store,
            encoder = BZ2Encoder,
            decoder = BZ2Decoder(),
            key = key,
            **extractor_args)

class JSONFeature(Feature):
    
    def __init__(\
            self,
            extractor,
            needs = None,
            store = False,
            key = None,
            **extractor_args):
        
        super(JSONFeature,self).__init__(\
            extractor,
            needs = needs,
            store = store,
            encoder = JSONEncoder,
            decoder = JSONDecoder(),
            key = key,
            **extractor_args)

class TextFeature(Feature):
    
    def __init__(\
             self,
             extractor,
             needs = None,
             store = False,
             key = None,
             **extractor_args):
        
        super(TextFeature,self).__init__(\
            extractor,
            needs = needs,
            store = store,
            encoder = TextEncoder,
            decoder = GreedyDecoder(),
            key = key,
            **extractor_args)



