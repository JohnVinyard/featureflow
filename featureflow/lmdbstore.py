import lmdb
from data import Database
from io import BytesIO
import os


class WriteStream(object):
    def __init__(self, key, env, db_getter=None):
        self.key = key
        self.db_getter = db_getter
        self.env = env
        self.buf = BytesIO()

    def __enter__(self):
        return self

    def __exit__(self, t, value, traceback):
        self.close()

    def close(self):
        _id, db = self.db_getter(self.key)
        self.buf.seek(0)
        data = self.buf.read()

        if not data:
            return

        with self.env.begin(db, write=True) as txn:
            txn.put(_id, data, db=db)

    def write(self, data):
        self.buf.write(data)


class LmdbDatabase(Database):
    def __init__(self, path, map_size=1000000000, key_builder=None):
        super(LmdbDatabase, self).__init__(key_builder=key_builder)
        self.path = path
        self.env = lmdb.open(
            self.path,
            max_dbs=10,
            map_size=map_size,
            writemap=True,
            map_async=True,
            metasync=True)
        self.env.reader_check()
        self.dbs = dict()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.env.close()

    def __del__(self):
        self.close()

    def _init_db_cache(self):
        with self.env.begin() as txn:
            cursor = txn.cursor()
            for feature in cursor.iternext(keys=True, values=False):
                self.dbs[feature] = self.env.open_db(feature)

    def _get_db(self, key):
        _id, feature, version = self.key_builder.decompose(key)
        versioned_key = self.key_builder.build(_id, version)
        try:
            return versioned_key, self.dbs[feature]
        except KeyError:
            db = self.env.open_db(feature)
            self.dbs[feature] = db
            return versioned_key, db

    def _get_read_db(self, key):
        _id, feature, version = self.key_builder.decompose(key)
        versioned_key = self.key_builder.build(_id, version)
        try:
            return versioned_key, self.dbs[feature]
        except KeyError:
            try:
                db = self.env.open_db(feature, create=False)
                self.dbs[feature] = db
                return versioned_key, self.dbs[feature]
            except lmdb.NotFoundError:
                raise KeyError(key)

    def write_stream(self, key, content_type):
        return WriteStream(key, self.env, self._get_db)

    def read_stream(self, key):
        _id, db = self._get_read_db(key)

        with self.env.begin(buffers=True) as txn:
            buf = txn.get(_id, db=db)
            if buf is None:
                raise KeyError(key)

        # POSSIBLE BUG:  Is it safe to keep the buffer around after the
        # transaction is complete?
        return BytesIO(buf)

    def size(self, key):
        _id, db = self._get_read_db(key)
        l = None

        with self.env.begin(buffers=True) as txn:
            buf = txn.get(_id, db=db)
            if buf is None:
                raise KeyError(key)
            l = len(buf)

        return l

    def _get_any_db(self):

        try:
            # return the first feature database
            # KLUDGE: This makes the assumption that features are never sparse,
            # i.e., all documents/ids have the same set of features
            return self.dbs.values()[0]
        except IndexError:
            pass

        # maybe another process has written data
        self._init_db_cache()

        try:
            return self.dbs.values()[0]
        except IndexError:
            return None

    def iter_ids(self):
        db = self._get_any_db()
        if db is None:
            return

        seen = set()
        with self.env.begin(db) as txn:
            cursor = txn.cursor(db)
            for _id in cursor.iternext(keys=True, values=False):
                _id, version = self.key_builder.decompose(_id)
                if _id in seen:
                    continue
                yield _id
                seen.add(_id)

    def __contains__(self, key):
        try:
            _id, db = self._get_read_db(key)
        except KeyError:
            return False
        with self.env.begin(buffers=True) as txn:
            buf = txn.get(_id, db=db)
        return buf is not None

    def __delitem__(self, key):
        try:
            _id, db = self._get_read_db(key)
        except KeyError:
            return
        with self.env.begin(write=True) as txn:
            txn.delete(_id, db=db)
