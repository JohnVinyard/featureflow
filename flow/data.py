from StringIO import StringIO
from uuid import uuid4
import os

from extractor import Node
from dependency_injection import dependency

class IdProvider(object):
    '''
    Marker class for object that returns new ids
    '''
    def new_id(self):
        raise NotImplemented()

class UuidProvider(IdProvider):

    def __init__(self):
        super(UuidProvider,self).__init__()

    def new_id(self, **kwargs):
        return uuid4().hex

class IntegerIdProvider(IdProvider):
    
    def __init__(self):
        super(IntegerIdProvider,self).__init__()
        self._id = 1
    
    def new_id(self, **kwargs):
        self._id += 1
        return self._id - 1

class UserSpecifiedIdProvider(IdProvider):
    
    def __init__(self, key = None):
        super(UserSpecifiedIdProvider,self).__init__()
        self._key = key
    
    def new_id(self, **kwargs):
        return kwargs[self._key]

class KeyBuilder(object):
    '''
    Marker class for an algorithm to build keys
    from "document" id and feature name
    '''
    def build(self,_id,feature_name):
        raise NotImplemented()
    
    def decompose(self,composed):
        raise NotImplemented()

class StringDelimitedKeyBuilder(KeyBuilder):

    def __init__(self,seperator = ':'):
        super(StringDelimitedKeyBuilder,self).__init__()
        self._seperator = seperator

    def build(self,_id,feature_name):
        return '{_id}{sep}{feature}'.format(\
            _id = _id,
            sep = self._seperator,
            feature = feature_name)
        
    def decompose(self,composed):
        return composed.split(self._seperator)

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

    def iter_ids(self):
        raise NotImplemented()

class IOWithLength(StringIO):
    
    def __init__(self, content):
        StringIO.__init__(self, content)
        self._length = len(content)
    
    def __len__(self):
        return self._length 

class InMemoryDatabase(Database):

    def __init__(self,name = None):
        super(InMemoryDatabase,self).__init__()
        self._dict = dict()
        self._name = name
    
    @property
    @dependency(KeyBuilder)
    def key_builder(self): pass

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
        return IOWithLength(self._dict[key])

    def iter_ids(self):
        seen = set()
        for key in self._dict.iterkeys():
            _id,_ = self.key_builder.decompose(key)
            if _id in seen: continue
            yield _id
            seen.add(_id)

class FileSystemDatabase(Database):
    
    def __init__(self, path):
        super(FileSystemDatabase,self).__init__()
        self._path = path
    
    @property
    @dependency(KeyBuilder)
    def key_builder(self): pass
    
    def write_stream(self,key,content_type):
        return open(os.path.join(self._path,key),'wb')
    
    def read_stream(self,key):
        try:
            return open(os.path.join(self._path,key),'rb')
        except IOError:
            raise KeyError(key)
    
    def iter_ids(self):
        seen = set()
        for fn in os.listdir(self._path):
            _id,_ = self.key_builder.decompose(fn)
            if _id in seen: continue
            yield _id
            seen.add(_id)

class DataWriter(Node):
    
    def __init__(\
            self, 
            needs = None,
            _id = None,  
            feature_name = None):
        
        super(DataWriter,self).__init__(needs = needs)
        self._id = _id
        self.feature_name = feature_name
        self.content_type = needs.content_type
        self._stream = None

    @property
    @dependency(KeyBuilder)
    def key_builder(self): pass
    
    @property
    def key(self):
        return self.key_builder.build(self._id,self.feature_name)
    
    @property
    @dependency(Database)
    def db(self): pass

    def __enter__(self):
        self._stream = self.db.write_stream(self.key,self.content_type)
        return self

    def __exit__(self,t,value,traceback):
        self._stream.close()
        
    def _process(self,data):
        yield self._stream.write(data)

class StringIODataWriter(Node):

    def __init__(self,needs = None,_id = None,feature_name = None):
        super(StringIODataWriter,self).__init__(needs = needs)
        self._id = _id
        self.feature_name = feature_name
        self.content_type = needs.content_type
        self._stream = StringIO()

    def _process(self,data):
        yield self._stream.write(data)