from types import GeneratorType
from util import PsychicIter,EmptyIterator
from collections import defaultdict
from itertools import izip

'''
goals
---------
What if I need to do something when I'm done processing, e.g., close a file?
multiple root extractors whose data is interleaved
I don't have to manage cache - DEFER
I can be a generator for all or part of my lifetime and not be a root
What python idioms could I be using to make this better?
'''
class Extractor(object):

    def __init__(self,needs = None):
        super(Extractor,self).__init__()
        self._cache = None
        self._listeners = []

        # This is ugly.  I need a better way to express this
        if needs is None:
            self._needs = []
        elif isinstance(needs,Extractor):
            self._needs = [needs]
        else:
            self._needs = needs
        # end ugliness

        for n in self._needs:
            n.add_listener(self)

    @property
    def is_root(self):
        return not self._needs

    def add_listener(self,listener):
        self._listeners.append(listener)

    def _enqueue(self,data,pusher):
        self._cache = data

    def _dequeue(self):
        if self._cache is None:
            raise NotEnoughData()

        v = self._cache
        self._cache = None
        return v

    def _process(self,data):
        yield data

    def _finalize(self,pusher):
        pass

    def _push(self,data):
        for l in self._listeners:
            [x for x in l.process(data,self)]

    def __finalize(self,pusher = None):
        self._finalize(pusher)
        for l in self._listeners:
            l.__finalize(self)

    def process(self,data = None,pusher = None):
        if data is not None:
            self._enqueue(data,pusher)

        try:
            data = self._process(self._dequeue())
            for d in data: 
                yield self._push(d)
        except NotEnoughData:
            yield None

        if self.is_root:
            self.__finalize()
            self._push(None)
            yield None


class NotEnoughData(Exception):
    pass

# class Extractor(object):
#     '''
#     Represents a Extractor in a directed graph whose Extractors represent data 
#     transformations, and whose vertices represent direct dependencies
#     '''
#     def __init__(self,needs = None):
#         super(Extractor,self).__init__()

#         # extractors that would like to transform my output
#         self._listeners = []
#         # a place to store data from extractors on which I depend
#         self._cache = None
#         # the extractors on which I depend
#         self._needs = needs
#         if not self._needs:
#             return
        
#         if isinstance(self._needs,Extractor):
#             self._needs = [self._needs]
        
#         for e in self._needs:
#             e.add_listener(self)
        
#     def add_listener(self,listener):
#         self._listeners.append(listener)

#     def find_listener(self,predicate):
#         for l in self._listeners:
#             if predicate(l):
#                 return l
#             else:
#                 return l.find_listener(predicate)
#         return None
    
#     def process(self,data,final_push = True,pusher = None):
#         print self,'process()'

#         if data:
#             self._update_cache(data,final_push,pusher)

#         if self._can_process(final_push):
#             data = self._process(final_push)
#             if isinstance(data,GeneratorType):
#                 g = PsychicIter(data)
#                 for d in g:
#                     self.push(d,g.done)
#             else:
#                 self.push(data,final_push)

#         if final_push: 
#             print self,'FINAL!!!!'
#             data = self._finalize()
#             self.push(data,final_push)

        
#         # wait until we have enough data to process
#         # if not self._can_process(final_push):
#         #     print self
#         #     return

#         # data = self._process(final_push)
#         # if not isinstance(data,GeneratorType):
#         #     self.push(data,final_push)
#         #     return
        
#         # for d in data:
#         #     self.push(d,False)
 
#         # # TODO: This is clumsy, and leads to bad code in inheriting classes.
#         # # Get rid of it.  Maybe a _finalize() method would be better
#         # if final_push:
#         #     #self.push(None,True)
#         #     print self,'finalize'
#         #     self._finalize()

#     def _finalize(self):
#         print 'final',self
#         raise NotImplemented()
    
#     # TODO: This should probably be "private"
#     def push(self,data,final_push):
#         for l in self._listeners:
#             # TODO: What if push just queued messages for all listeners, instead
#             # of calling process() directly?  I could just write different queues,
#             # including a synchronous one, which would call process directly
#             l.process(data,final_push,self)
    
#     def _update_cache(self,data,final_push,pusher):
#         '''
#         Update the data we're keeping stored until we can do some processing
#         '''
#         self._cache = data
    
#     def _process(self,final_push):
#         '''
#         do work
#         '''
#         return self._cache
     
#     def _can_process(self,final_push):
#         '''
#         Do we have enough data in _cache to do some work?
#         '''
#         return bool(self._cache) or final_push


class Numbers(Extractor):

    def __init__(self):
        super(Numbers,self).__init__()

    def _process(self,data):
        for i in xrange(*data):
            yield i

class DumbPrinter(Extractor):

    def __init__(self,needs = None):
        super(DumbPrinter,self).__init__(needs = needs)

    def _process(self,data):
        print data
        yield data

class SmartPrinter(Extractor):

    def __init__(self,needs = None):
        super(SmartPrinter,self).__init__(needs = needs)
        self._cache = []

    def _finalize(self,pusher):
        r = 3 - len(self._cache)
        self._cache.extend([0] * r)

    def _enqueue(self,data,pusher):
        self._cache.append(data)

    def _dequeue(self):
        if len(self._cache) < 3:
            raise NotEnoughData()

        for i in xrange(0,len(self._cache),3):
            sl = self._cache[i : i +3]
            if len(sl) < 3:
                self._cache = self._cache[i:]
                break
            yield sl

        self._cache = self._cache[i + 3:]

    def _process(self,data):
        results  = list(data)
        print results
        yield results

class AllAtOncePrinter(Extractor):

    def __init__(self,needs = None):
        super(AllAtOncePrinter,self).__init__(needs = needs)
        self._cache = []
        self._final = False

    def _finalize(self,pusher):
        self._final = True

    def _enqueue(self,data,pusher):
        self._cache.append(data)

    def _dequeue(self):
        if not self._final:
            raise NotEnoughData()

        return super(AllAtOncePrinter,self)._dequeue()

    def _process(self,data):
        print data
        yield data

class PairPrinter(Extractor):

    def __init__(self,needs = None):
        super(PairPrinter,self).__init__(needs = needs)
        self._cache = defaultdict(list)

    def _enqueue(self,data,pusher):
        self._cache[id(pusher)].append(data)

    def _dequeue(self):
        if len(self._cache) < len(self._needs):
            raise NotEnoughData()

        try:
            return [l.pop(0) for l in self._cache.itervalues()]
        except IndexError:
            raise NotEnoughData()

    def _process(self,data):
        print data
        return data
        

if __name__ == '__main__':
    src = Numbers()
    src2 = Numbers()
    dp = DumbPrinter(needs = src)
    sp = SmartPrinter(needs = src)
    ap = AllAtOncePrinter(needs = src)
    pp = PairPrinter(needs = [src,src2])
    
    i1 = src.process((10,))
    i2 = src2.process((10,20))
    i3 = izip(i1,i2)
    [x for x in i3]
