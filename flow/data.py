from extractor import Extractor

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