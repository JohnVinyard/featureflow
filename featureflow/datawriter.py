from cStringIO import StringIO
from extractor import Node
import json


class BaseDataWriter(Node):
    def __init__(self, needs=None, key_builder=None, database=None):
        super(BaseDataWriter, self).__init__(needs=needs)
        self.key_builder = key_builder
        self.database = database


class DataWriter(BaseDataWriter):
    def __init__(
            self,
            needs=None,
            _id=None,
            feature_name=None,
            feature_version=None,
            key_builder=None,
            database=None,
            event_log=None):
        super(DataWriter, self).__init__(
            needs=needs,
            key_builder=key_builder,
            database=database)

        self.event_log = event_log
        self.feature_version = feature_version
        self._id = _id
        self.feature_name = feature_name
        self.content_type = needs.content_type

    @property
    def key(self):
        assert self.feature_version is not None
        return self.key_builder.build(
            self._id, self.feature_name, self.feature_version)

    def __enter__(self):
        self._stream = self.database.write_stream(self.key, self.content_type)
        return self

    def __exit__(self, t, value, traceback):
        try:
            self._stream.close()
        except (IOError, ValueError):
            pass
        if self.event_log is not None:
            self.event_log.append(
                json.dumps({
                    '_id': self._id,
                    'name': self.feature_name,
                    'version': self.feature_version
                }))

    def _process(self, data):
        yield self._stream.write(data)


class ClobberDataWriter(DataWriter):
    def __init__(
            self,
            needs=None,
            _id=None,
            feature_name=None,
            feature_version=None,
            key_builder=None,
            database=None,
            event_log=None):
        super(ClobberDataWriter, self).__init__(
            needs=needs,
            _id=_id,
            feature_name=feature_name,
            feature_version=feature_version,
            key_builder=key_builder,
            database=database,
            event_log=event_log)

    def _process(self, data):
        self._stream = self.database.write_stream(self.key, self.content_type)
        x = self._stream.write(data)
        self._stream.close()
        yield x


class StringIODataWriter(BaseDataWriter):
    def __init__(
            self,
            needs=None,
            _id=None,
            feature_name=None,
            feature_version=None,
            key_builder=None,
            database=None,
            event_log=None):
        super(StringIODataWriter, self).__init__(
            needs=needs,
            key_builder=key_builder,
            database=database)

        self.event_log = event_log
        self.feature_version = feature_version
        self._id = _id
        self.feature_name = feature_name
        self.content_type = needs.content_type
        self._stream = StringIO()

    def _process(self, data):
        yield self._stream.write(data)
