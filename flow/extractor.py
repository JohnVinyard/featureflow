from types import GeneratorType

class Extractor(object):
    '''
    Represents a node in a directed graph whose nodes represent data 
    transformations, and whose vertices represent direct dependencies
    '''
    def __init__(self,needs = None):
        super(Extractor,self).__init__()
        # extractors that would like to transform my output
        self._listeners = []
        # a place to store data from extractors on which I depend
        self._cache = None
        # the extractors on which I depend
        self._needs = needs
        if not self._needs:
            return
        
        if isinstance(self._needs,Extractor):
            self._needs = [self._needs]
        
        for e in self._needs:
            e.add_listener(self)
        
    def add_listener(self,listener):
        self._listeners.append(listener)

    def find_listener(self,predicate):
        for l in self._listeners:
            if predicate(l):
                return l
            else:
                return l.find_listener(predicate)
        return None
    
    def process(self,data,final_push = True,pusher = None):
        self._update_cache(data,final_push,pusher)
        
        if not self._can_process(final_push):
            return
        
        data = self._process(final_push)
        if not isinstance(data,GeneratorType):
            self.push(data,final_push)
            return
        
        for d in data:
            self.push(d,False)
 
        # TODO: This is clumsy, and leads to bad code in inheriting classes.
        # Get rid of it.  Maybe a _finalize() method would be better
        if final_push:
            self.push(None,True)
    
    # TODO: This should probably be "private"
    def push(self,data,final_push):
        for l in self._listeners:
            # TODO: What if push just queued messages for all listeners, instead
            # of calling process() directly?  I could just write different queues,
            # including a synchronous one, which would call process directly
            l.process(data,final_push,self)
    
    def _update_cache(self,data,final_push,pusher):
        '''
        Update the data we're keeping stored until we can do some processing
        '''
        self._cache = data
    
    def _process(self,final_push):
        '''
        do work
        '''
        return self._cache
     
    def _can_process(self,final_push):
        '''
        Do we have enough data in _cache to do some work?
        '''
        return bool(self._cache) or final_push