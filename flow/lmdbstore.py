import lmdb
from data import Database, KeyBuilder
from dependency_injection import dependency
from io import BytesIO

class WriteStream(object):
    
    def __init__(self, _id, env, db = None):
        self._id = _id
        self.db = db
        self.env = env
        self.buf = BytesIO()

    def __enter__(self):
        return self

    def __exit__(self, t, value, traceback):
        self.close()
    
    def close(self):
        self.buf.seek(0)
        with self.env.begin(write = True) as txn:
            txn.put(self._id, self.buf.read(), db = self.db)
    
    def write(self, data):
        self.buf.write(data)

class ReadStream(object):
    
    def __init__(self, buf):
        self.buf = buf
        self.pos = 0
    
    def __enter__(self):
        return self
    
    def __exit__(self, t, value, traceback):
        pass
    
    def read(self, nbytes = None):
        if nbytes is None:
            nbytes = len(self.buf)
        v = buffer(self.buf, self.pos, nbytes)
        self.pos += nbytes
        return v

class LmdbDatabase(Database):
    
    def __init__(self, path, map_size = 1000000000):
        super(Database, self).__init__()
        self.path = path
        self.env = lmdb.open(\
             self.path, 
             max_dbs = 10, 
             map_size = map_size,
             writemap = True,
             map_async = True,
             metasync = False)
        self.dbs = dict()
    
    def _get_db(self, key):
        _id, feature = self.key_builder.decompose(key)
        try:
            return _id, self.dbs[feature]
        except KeyError:
            db = self.env.open_db(feature)
            self.dbs[feature] = db
            return _id, db
    
    @property
    @dependency(KeyBuilder)
    def key_builder(self): pass
    
    def write_stream(self, key, content_type):
        _id, db = self._get_db(key)
        return WriteStream(_id, self.env, db)
    
    def read_stream(self, key):
        _id, db = self._get_db(key)
        with self.env.begin(buffers = True) as txn:
            buf = txn.get(_id, db = db)
        if buf is None:
            raise KeyError(key)
        return ReadStream(buf)
    
    def iter_ids(self):
        db = self.dbs.values()[0]
        with self.env.begin() as txn:
            cursor = txn.cursor(db)
            for _id in cursor.iternext(keys = True, values = False):
                yield _id

    def __contains__(self, key):
        _id, db = self._get_db(key)
        with self.env.begin(buffers = True) as txn:
            buf = txn.get(_id, db = db)
        return buf is not None