from extractor import Graph
from feature import Feature
from persistence import PersistenceSettings


class MetaModel(type):
    def __init__(cls, name, bases, attrs):
        cls.features = {}
        cls._add_features(cls.features)
        super(MetaModel, cls).__init__(name, bases, attrs)

    def iter_features(self):
        return self.features.itervalues()

    def _add_features(cls, features):
        for k, v in cls.__dict__.iteritems():
            if not isinstance(v, Feature):
                continue
            v.key = k
            features[k] = v

        for b in cls.__bases__:
            try:
                b._add_features(features)
            except AttributeError:
                pass


class NoPersistenceSettingsError(Exception):
    """
    Error raised when a BaseModel-derived class is used without an accompanying
    PersistenceSettings sub-class.
    """
    pass


class BaseModel(object):
    __metaclass__ = MetaModel

    def __init__(self, _id=None):
        super(BaseModel, self).__init__()
        if _id:
            self._id = _id

    @staticmethod
    def _ensure_persistence_settings(cls):
        if not issubclass(cls, PersistenceSettings):
            raise NoPersistenceSettingsError(
                    'The class {cls} is not a PersistenceSettings subclass'
                        .format(cls=cls.__name__))

    def __getattribute__(self, key):
        f = object.__getattribute__(self, key)

        if not isinstance(f, Feature):
            return f

        BaseModel._ensure_persistence_settings(self.__class__)
        feature = getattr(self.__class__, key)
        decoded = feature.__call__(self._id, persistence=self.__class__)
        setattr(self, key, decoded)
        return decoded

    @classmethod
    def _build_extractor(cls, _id):
        g = Graph()
        for feature in cls.features.itervalues():
            feature._build_extractor(_id, g, cls)
        return g

    @classmethod
    def process(cls, **kwargs):
        BaseModel._ensure_persistence_settings(cls)
        _id = cls.id_provider.new_id(**kwargs)
        graph = cls._build_extractor(_id)
        graph.remove_dead_nodes(cls.features.itervalues())
        try:
            graph.process(**kwargs)
            return _id
        except Exception as e:
            for f in cls.features.itervalues():
                if not f.store:
                    continue
                key = cls.key_builder.build(_id, f.key, f.version)
                try:
                    del cls.database[key]
                except:
                    pass
            raise e
