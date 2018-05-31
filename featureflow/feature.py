from var import Var
from collections import OrderedDict
import inspect
from datawriter import DataWriter, StringIODataWriter, ClobberDataWriter
from decoder import JSONDecoder, Decoder, GreedyDecoder, DecoderNode, \
    BZ2Decoder, PickleDecoder
from encoder import IdentityEncoder, JSONEncoder, TextEncoder, BZ2Encoder, \
    PickleEncoder
from extractor import Graph, FunctionalNode, Node, KeySelector
from util import dictify


class Feature(object):
    def __init__(
            self,
            extractor,
            needs=None,
            store=False,
            encoder=None,
            decoder=None,
            key=None,
            data_writer=None,
            persistence=None,
            **extractor_args):

        super(Feature, self).__init__()
        self.key = key
        self.extractor = extractor
        self.store = store
        self.encoder = encoder or IdentityEncoder

        self.needs = needs

        self.decoder = decoder or Decoder()
        self.extractor_args = extractor_args

        self._handle_callable_extractor()

        self.persistence = persistence

        if data_writer:
            self._data_writer = data_writer
        else:
            self._data_writer = DataWriter

    def aspect(self, aspect_key):
        return Aspect(aspect_key, self)

    def _fixup_needs(self):
        self.needs = dictify(self.needs, lambda item: item.key)

        for k, v in self.needs.iteritems():
            try:
                # the value is an Aspect
                self.needs[k] = v.feature

                class AspectExtractor(KeySelector, self.extractor):
                    def __init__(self, *args, **kwargs):
                        super(AspectExtractor, self).__init__(*args, **kwargs)

                self.extractor = AspectExtractor
                self.extractor_args.update(aspect_key=v.aspect_key)
            except AttributeError:
                # the value is already just a feature
                pass

    def _handle_callable_extractor(self):
        if inspect.isclass(self.extractor) \
                and issubclass(self.extractor, Node):
            return

        if callable(self.extractor):
            self.extractor_args = dict(
                func=self.extractor, **self.extractor_args)
            self.extractor = FunctionalNode
            return

        raise ValueError('extractor must be either a Node or a callable')

    def __repr__(self):
        return '{cls}(key = {key}, store = {store})'.format(
            cls=self.__class__.__name__, **self.__dict__)

    def __str__(self):
        return self.__repr__()

    @property
    def dependencies(self):
        return self.needs.values()

    @property
    def version(self):
        # KLUDGE: Build a shallow version of the extractor.  Building a deep
        # version with re-usable code is more difficult, because
        # self._build_extractor relies on this version property, so there's
        # a circular dependency.
        if isinstance(self.needs, dict):
            dependencies = dict(
                (k, v.extractor(**v.extractor_args))
                for k, v in self.needs.iteritems())
        else:
            dependencies = [f.extractor(**f.extractor_args) for f in self.needs]

        e = self.extractor(needs=dependencies, **self.extractor_args)
        return e.version

    def copy(
            self,
            extractor=None,
            needs=None,
            store=None,
            data_writer=None,
            persistence=None,
            extractor_args=None):
        """
        Use self as a template to build a new feature, replacing
        values in kwargs
        """
        f = Feature(
            extractor or self.extractor,
            needs=needs,
            store=self.store if store is None else store,
            encoder=self.encoder,
            decoder=self.decoder,
            key=self.key,
            data_writer=data_writer,
            persistence=persistence,
            **(extractor_args or self.extractor_args))
        f._fixup_needs()
        return f

    def database(self, persistence):
        return (self.persistence or persistence).database

    def keybuilder(self, persistence):
        return (self.persistence or persistence).key_builder

    def event_log(self, persistence):
        try:
            return (self.persistence or persistence).event_log
        except AttributeError:
            return None

    def feature_key(self, _id, persistence):
        return self.keybuilder(persistence).build(_id, self.key, self.version)

    def reader(self, _id, key, persistence):
        key = self.keybuilder(persistence).build(_id, key, self.version)
        return self.database(persistence).read_stream(key)

    @property
    def is_root(self):
        return not self.needs

    def _stored(self, _id, persistence):
        key = self.keybuilder(persistence).build(_id, self.key, self.version)
        return key in self.database(persistence)

    @property
    def content_type(self):
        return self.encoder.content_type

    def _can_compute(self, _id, persistence):
        """
        Return true if this feature stored, or is unstored, but can be computed
        from stored dependencies
        """
        if self.store and self._stored(_id, persistence):
            return True

        if self.is_root:
            return False

        return all(
            [n._can_compute(_id, persistence) for n in self.dependencies])

    def _compute(self, _id=None, persistence=None):
        graph, stream = self._build_partial(_id, persistence)

        kwargs = dict()
        for k, extractor in graph.roots().iteritems():
            try:
                kwargs[k] = extractor._reader
            except AttributeError:
                kwargs[k] = self.reader(_id, k, persistence)

        graph.process(**kwargs)
        return stream

    def compute(self, _id, persistence):
        self._compute(_id, persistence)

    def __call__(self, _id=None, decoder=None, persistence=None):
        if decoder is None:
            decoder = self.decoder

        try:
            raw = self.reader(_id, self.key, persistence)
            decoded = decoder(raw)
            return decoded
        except KeyError:
            pass

        if not self._can_compute(_id, persistence):
            raise AttributeError('%s cannot be computed' % self.key)

        stream = self._compute(_id, persistence)

        if stream is None:
            stream = self.reader(_id, self.key, persistence)

        stream.seek(0)
        decoded = decoder(stream)
        return decoded

    def _build_partial(self, _id, persistence):
        features = self._partial(_id, persistence=persistence)
        g = Graph()
        for feat in features.itervalues():
            e = feat._build_extractor(_id, g, persistence)
            if feat.key == self.key:
                stream = e.find_listener(
                    lambda x: isinstance(x, StringIODataWriter))
                if stream is not None:
                    stream = stream._stream

        return g, stream

    def _partial(self, _id, features=None, persistence=None):
        """
        TODO: _partial is a shit name for this, kind of.  I'm building a graph
        such that I can only do work necessary to compute self, and no more
        """

        root = features is None

        stored = self._stored(_id, persistence)
        is_cached = self.store and stored

        if self.store and not stored:
            data_writer = None
        elif root:
            data_writer = StringIODataWriter
        else:
            data_writer = None

        should_store = self.store and not stored
        nf = self.copy(
            extractor=DecoderNode if is_cached else self.extractor,
            store=root or should_store,
            needs=dict(),
            data_writer=data_writer,
            persistence=self.persistence,
            extractor_args=dict(decodifier=self.decoder, version=self.version) \
                if is_cached else self.extractor_args)

        if root:
            features = dict()

        features[self.key] = nf

        if not is_cached:
            for k, v in self.needs.iteritems():
                v._partial(_id, features=features, persistence=persistence)
                nf.needs[k] = features[v.key]

        return features

    def _depends_on(self, _id, graph, persistence, **kwargs):
        needs = OrderedDict()

        for k, f in self.needs.iteritems():
            if f.key in graph:
                needs[k] = graph[f.key]
                continue

            e = f._build_extractor(_id, graph, persistence, **kwargs)
            needs[k] = e

        return needs

    def _build_extractor(self, _id, graph, persistence, **kwargs):
        try:
            return graph[self.key]
        except KeyError:
            pass

        needs = self._depends_on(_id, graph, persistence, **kwargs)

        extractor_args = dict()
        for k, v in self.extractor_args.iteritems():
            if isinstance(v, Var) and v.name == k:
                try:
                    extractor_args[k] = kwargs[k]
                except KeyError:
                    raise ValueError('{k} is a Var, but it was not provided'
                                     .format(**locals()))
            else:
                extractor_args[k] = v

        e = self.extractor(needs=needs, **extractor_args)

        if isinstance(e, DecoderNode):
            reader = self.reader(_id, self.key, persistence)
            setattr(e, '_reader', reader)

        graph[self.key] = e

        if not self.store:
            return e

        key = self.key
        encoder = self.encoder(needs=e)
        graph['{key}_encoder'.format(**locals())] = encoder

        dw = self._data_writer(
            needs=encoder,
            _id=_id,
            feature_name=self.key,
            feature_version=self.version,
            key_builder=self.keybuilder(persistence),
            database=self.database(persistence),
            event_log=self.event_log(persistence))

        graph['{key}_writer'.format(**locals())] = dw
        return e


class Aspect(object):
    def __init__(self, aspect_key, feature):
        super(Aspect, self).__init__()
        self.aspect_key = aspect_key
        self.feature = feature

    @property
    def key(self):
        return self.feature.key


class CompressedFeature(Feature):
    def __init__(
            self,
            extractor,
            needs=None,
            store=False,
            key=None,
            **extractor_args):
        super(CompressedFeature, self).__init__(
            extractor,
            needs=needs,
            store=store,
            encoder=BZ2Encoder,
            decoder=BZ2Decoder(),
            key=key,
            **extractor_args)


class PickleFeature(Feature):
    def __init__(
            self,
            extractor,
            needs=None,
            store=False,
            key=None,
            data_writer=None,
            **extractor_args):
        super(PickleFeature, self).__init__(
            extractor,
            needs=needs,
            store=store,
            encoder=PickleEncoder,
            decoder=PickleDecoder(),
            key=key,
            data_writer=data_writer,
            **extractor_args)


class ClobberPickleFeature(PickleFeature):
    def __init__(
            self,
            extractor,
            needs=None,
            store=False,
            key=None,
            **extractor_args):
        super(ClobberPickleFeature, self).__init__(
            extractor,
            needs=needs,
            store=store,
            key=key,
            data_writer=ClobberDataWriter,
            **extractor_args)


class JSONFeature(Feature):
    def __init__(
            self,
            extractor,
            needs=None,
            store=False,
            key=None,
            encoder=JSONEncoder,
            data_writer=None,
            **extractor_args):
        super(JSONFeature, self).__init__(
            extractor,
            needs=needs,
            store=store,
            encoder=encoder,
            decoder=JSONDecoder(),
            key=key,
            data_writer=data_writer,
            **extractor_args)


class ClobberJSONFeature(JSONFeature):
    def __init__(
            self,
            extractor,
            needs=None,
            store=False,
            key=None,
            encoder=JSONEncoder,
            **extractor_args):
        super(ClobberJSONFeature, self).__init__(
            extractor,
            needs=needs,
            store=store,
            encoder=encoder,
            key=key,
            data_writer=ClobberDataWriter,
            **extractor_args)


class TextFeature(Feature):
    def __init__(
            self,
            extractor,
            needs=None,
            store=False,
            key=None,
            **extractor_args):
        super(TextFeature, self).__init__(
            extractor,
            needs=needs,
            store=store,
            encoder=TextEncoder,
            decoder=GreedyDecoder(),
            key=key,
            **extractor_args)
