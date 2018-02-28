from extractor import Graph
from feature import Feature
from persistence import PersistenceSettings
from random import choice


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

        for f in cls.features.values():
            f._fixup_needs()

    @staticmethod
    def _ensure_persistence_settings(cls):
        if not issubclass(cls, PersistenceSettings):
            raise NoPersistenceSettingsError(
                'The class {cls} is not a PersistenceSettings subclass'
                    .format(cls=cls.__name__))

    def __iter__(cls):
        cls._ensure_persistence_settings(cls)
        for _id in cls.database:
            yield cls(_id)


class NoPersistenceSettingsError(Exception):
    """
    Error raised when a BaseModel-derived class is used without an accompanying
    PersistenceSettings sub-class.
    """
    pass


class ModelExistsError(Exception):
    """
    Error raised when a model has already been computed and persisted
    """
    pass


class BaseModel(object):
    __metaclass__ = MetaModel

    def __init__(self, _id=None):
        super(BaseModel, self).__init__()
        if _id:
            self._id = _id

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
    def _build_extractor(cls, _id, **kwargs):
        g = Graph()
        for feature in cls.features.itervalues():
            feature._build_extractor(_id, g, cls, **kwargs)
        return g

    @classmethod
    def random(cls):
        all_ids = list(cls.database.iter_ids())
        return cls(_id=choice(all_ids))

    @classmethod
    def exists(cls, _id=None, feature=None):
        if feature and not feature.store:
            raise ValueError('feature must have store=True')

        if _id is None:
            try:
                _id = cls.id_provider.new_id()
            except:
                raise ValueError(
                    '_id must be provided explicitly, or it must be static')

        feature = feature or filter(lambda f: f.store, cls.iter_features())[0]
        feature_key = feature.feature_key(_id, cls)
        return feature_key in cls.database

    @classmethod
    def process(cls, raise_if_exists=False, **kwargs):
        BaseModel._ensure_persistence_settings(cls)
        _id = cls.id_provider.new_id(**kwargs)

        if raise_if_exists and cls.exists(_id):
            # # choose a random, stored feature
            # feature = filter(lambda f: f.store, cls.iter_features())[0]
            # feature_key = feature.feature_key(_id, cls)
            # # check if that feature is already stored
            # if feature_key in cls.database:
            raise ModelExistsError(
                '{_id} is already stored in the database'.format(
                    **locals()))

        try:
            del kwargs['_id']
        except KeyError:
            pass

        graph = cls._build_extractor(_id, **kwargs)
        graph.remove_dead_nodes(cls.features.itervalues())

        graph.process(**kwargs)
        return _id
