from extractor import Extractor
from StringIO import StringIO

class Database(object):
    '''
    Marker class for a datastore
    '''
    # TODO: Maybe this should just be open(), since it returns a file-like 
    # object
    def write_stream(self,key,content_type):
        raise NotImplemented()
    
    # TODO: Maybe this should just be open(), since it returns a file-like
    # object
    def read_stream(self,key):
        raise NotImplemented()

class InMemoryDatabase(object):

    def __init__(self):
        super(InMemoryDatabase,self).__init__()
        self._dict = dict()

    def write_stream(self,key,content_type):
        sio = StringIO()
        self._dict[key] = sio
        def hijacked_close():
            sio.seek(0)
            self._dict[key] = sio.read()
            sio._old_close()
        sio._old_close = sio.close
        sio.close = hijacked_close
        return sio

    def read_stream(self,key):
        return StringIO(self._dict[key])
    
class DataWriter(Extractor):
    '''
    Marker class for object that writes to the datastore
    '''
    def __init__(self, needs = None, _id = None, feature_name = None):
        super(DataWriter,self).__init__(needs = needs)
    
class DataReader(object):
    '''
    Marker class for object that reads from the datastore
    '''
    def __init__(self,_id = None, feature = None):
        super(DataReader,self).__init__()

class IdProvider(object):
    '''
    Marker class for object that returns new ids
    '''
    def new_id(self):
        raise NotImplemented()