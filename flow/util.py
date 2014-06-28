
def chunked(f, chunksize = 4096):
    data = f.read(chunksize)
    while data:
        yield data
        data = f.read(chunksize)

class EmptyIterator(object):

    def __init__(self):
        super(EmptyIterator,self).__init__()

    def __iter__(self):
        return
        yield

class PsychicIter(object):
    '''
    An iterator that can warn, if asked, that the next call to next() will raise
    a StopIteration exception.  This sort of goes against the whole notion of
    iterators, but it's helpful.
    '''
    def __init__(self,iterator):
        self.done = False
        self._iter = iterator
        self._buffer = []
    
    def __iter__(self):
        while True:
            yield self.next()
    
    def next(self):
        for i in range(0,2 - len(self._buffer)):
            try:
                self._buffer.append(self._iter.next())
            except StopIteration:
                self.done = True
                break
            
        try:
            return self._buffer.pop(0)
        except IndexError:
            raise StopIteration