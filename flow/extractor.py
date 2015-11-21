from itertools import izip_longest
import contextlib

 
class Node(object):

    def __init__(self,needs = None):
        super(Node,self).__init__()
        self._cache = None
        self._listeners = []

        if needs is None:
            self._needs = []
        elif isinstance(needs,Node):
            self._needs = [needs]
        else:
            self._needs = needs

        for n in self._needs:
            n.add_listener(self)
        
        self._finalized_dependencies = set()
        self._enqueued_dependencies = set()

    def __repr__(self):
        return self.__class__.__name__
    
    def __str__(self):
        return self.__repr__()

    def __enter__(self):
        return self

    def __exit__(self,t,value,traceback):
        pass
    
    @property
    def needs(self):
        return self._needs
    
    @property
    def dependency_count(self):
        return len(self._needs)

    @property
    def is_root(self):   
        return not self._needs
    
    @property
    def is_leaf(self):
        return not self._listeners

    def add_listener(self,listener):
        self._listeners.append(listener)

    def find_listener(self,predicate):
        for l in self._listeners:
            if predicate(l):
                return l
            else:
                return l.find_listener(predicate)
        return None
    
    def disconnect(self):
        for e in self.needs:
            e._listeners.remove(self)

    def _enqueue(self,data,pusher):
        self._cache = data

    def _dequeue(self):
        if self._cache is None:
            raise NotEnoughData()

        v, self._cache = self._cache, None
        return v

    def _process(self,data):
        yield data
    
    def _first_chunk(self, data):
        return data
    
    def _last_chunk(self):
        return iter(())

    def _finalize(self,pusher):
        pass
    
    @property
    def _finalized(self):
        '''
        Return true if all dependencies have informed this node that they'll
        be sending no more data (by calling _finalize()), and that they have
        sent at least one batch of data (by calling enqueue())
        '''
        return \
            len(self._finalized_dependencies) >= self.dependency_count \
            and len(self._enqueued_dependencies) >= self.dependency_count 

    def _push(self,data):
        for l in self._listeners:
            [x for x in l.process(data,self)]

    def __finalize(self,pusher = None):
        self._finalize(pusher)
        if pusher in self._needs:
            self._finalized_dependencies.add(id(pusher))
        if pusher: return
        for l in self._listeners:
            l.__finalize(self)

    def process(self,data = None,pusher = None):
        if data is not None:
            self._enqueued_dependencies.add(id(pusher))
            self._enqueue(data,pusher)

        try:
            inp = self._dequeue()
            inp = self._first_chunk(inp)
            self._first_chunk = lambda x : x
            data = self._process(inp)
            for d in data: yield self._push(d)
        except NotEnoughData:
            yield None

        if self.is_root or self._finalized:
            chunks = self._last_chunk()
            for chunk in chunks: self._push(chunk)
            self.__finalize()
            self._push(None)
            yield None

class Aggregator(object):
    '''
    A mixin for Node-derived classes that addresses the case when the processing
    node cannot do its computation until all input has been received
    '''
    def __init__(self,needs = None):
        super(Aggregator,self).__init__(needs = needs)
    def _dequeue(self):
        if not self._finalized:
            raise NotEnoughData()
        return super(Aggregator,self)._dequeue() 
        

class NotEnoughData(Exception):
    '''
    Exception thrown by extractors when they do not yet have enough data to 
    execute the processing step
    ''' 
    pass


class Graph(dict):

    def __init__(self,**kwargs):
        super(Graph,self).__init__(**kwargs)

    def roots(self):
        return dict((k,v) for k,v in self.iteritems() if v.is_root)
    
    def leaves(self):
        return dict((k,v) for k,v in self.iteritems() if v.is_leaf)

    def remove_dead_nodes(self, features):
        for feature in features:
            extractor = self[feature.key]
            if extractor.is_leaf and not feature.store:
                extractor.disconnect()

    def process(self,**kwargs):
        # get all root nodes (those that produce data, rather than consuming 
        # it)
        roots = self.roots()

        # ensure that kwargs contains *at least* the arguments needed for the
        # root nodes
        intersection = set(kwargs.keys()) & set(roots.keys())
        if len(intersection) < len(roots):
            raise KeyError(\
               ('the keys {kw} were provided to the process() method, but the' \
               + ' keys for the root extractors were {r}')\
               .format(kw = kwargs.keys(),r = roots.keys()))
        
        graph_args = dict((k,kwargs[k]) for k in intersection)
                    
        # get a generator for each root node.
        generators = [roots[k].process(v) for k,v in graph_args.iteritems()]
        with contextlib.nested(*self.values()) as _:
            [x for x in izip_longest(*generators)] 

