from itertools import izip_longest
import contextlib
from collections import deque, defaultdict
import inspect


class InvalidProcessMethod(Exception):
    """
    Exception thrown when the _process method of an Node is not a generator
    """

    def __init__(self, cls):
        msg = '{name}._process method must be a generator' \
            .format(name=cls.__name__)
        super(InvalidProcessMethod, self).__init__(msg)


class Node(object):
    def __init__(self, needs=None):
        super(Node, self).__init__()
        if not inspect.isgeneratorfunction(self._process):
            raise InvalidProcessMethod(self.__class__)

        self._cache = None
        self._listeners = []

        try:
            self._needs = list(needs)
        except TypeError:
            self._needs = [] if needs is None else [needs]

        for n in self._needs:
            n.add_listener(self)

        self._finalized_dependencies = set()
        self._enqueued_dependencies = set()

    def __repr__(self):
        return self.__class__.__name__

    def __str__(self):
        return self.__repr__()

    def __enter__(self):
        return self

    def __exit__(self, t, value, traceback):
        pass

    @property
    def version(self):
        return self.__class__.__name__

    @property
    def needs(self):
        return self._needs

    @property
    def dependency_count(self):
        return len(self._needs)

    @property
    def is_root(self):
        return not self._needs

    @property
    def is_leaf(self):
        return not self._listeners

    def add_listener(self, listener):
        self._listeners.append(listener)

    def find_listener(self, predicate):
        for l in self._listeners:
            return l if predicate(l) else l.find_listener(predicate)
        return None

    def disconnect(self):
        for e in self.needs:
            e._listeners.remove(self)

    def _enqueue(self, data, pusher):
        self._cache = data

    def _dequeue(self):
        if self._cache is None:
            raise NotEnoughData()

        v, self._cache = self._cache, None
        return v

    def _process(self, data):
        yield data

    def _first_chunk(self, data):
        return data

    def _last_chunk(self):
        return iter(())

    def _finalize(self, pusher):
        pass

    @property
    def _finalized(self):
        """
        Return true if all dependencies have informed this node that they'll
        be sending no more data (by calling _finalize()), and that they have
        sent at least one batch of data (by calling enqueue())
        """
        return \
            len(self._finalized_dependencies) >= self.dependency_count \
            and len(self._enqueued_dependencies) >= self.dependency_count

    def _push(self, data, queue=None):
        queue.appendleft((
            id(self),
            self.process.__name__,
            {'pusher': self, 'data': data, 'queue': queue}))

    def _finish(self, pusher=None, queue=None):
        self._finalize(pusher)
        if pusher in self._needs:
            self._finalized_dependencies.add(id(pusher))
        if pusher:
            return
        queue.appendleft((
            id(self),
            self._finish.__name__,
            {'pusher': self, 'queue': queue}))

    def process(self, data=None, pusher=None, queue=None):
        if data is not None:
            self._enqueued_dependencies.add(id(pusher))
            self._enqueue(data, pusher)

        try:
            inp = self._dequeue()
            inp = self._first_chunk(inp)
            self._first_chunk = lambda x: x
            for d in self._process(inp):
                yield self._push(d, queue=queue)
        except NotEnoughData:
            yield None

        if self.is_root or self._finalized:
            for chunk in self._last_chunk():
                yield self._push(chunk, queue=queue)
            self._finish(pusher=None, queue=queue)
            self._push(None, queue=queue)
            yield None


class Aggregator(object):
    """
    A mixin for Node-derived classes that addresses the case when the processing
    node cannot do its computation until all input has been received
    """

    def __init__(self, needs=None):
        super(Aggregator, self).__init__(needs=needs)

    def _dequeue(self):
        if not self._finalized:
            raise NotEnoughData()
        return super(Aggregator, self)._dequeue()


class NotEnoughData(Exception):
    """
    Exception thrown by extractors when they do not yet have enough data to
    execute the processing step
    """
    pass


class Graph(dict):
    def __init__(self, **kwargs):
        super(Graph, self).__init__(**kwargs)

    def roots(self):
        return dict((k, v) for k, v in self.iteritems() if v.is_root)

    def leaves(self):
        return dict((k, v) for k, v in self.iteritems() if v.is_leaf)

    def subscriptions(self):
        subscriptions = defaultdict(list)
        for node in self.itervalues():
            for n in node._needs:
                subscriptions[id(n)].append(node)
        return subscriptions

    def remove_dead_nodes(self, features):
        # starting from the leaves, remove any nodes that are not stored, and 
        # have no stored consuming nodes
        mapping = dict((self[f.key], f) for f in features)
        nodes = deque(self.leaves().values())
        while nodes:
            extractor = nodes.pop()
            nodes.extendleft(extractor.needs)
            try:
                feature = mapping[extractor]
            except KeyError:
                continue
            if extractor.is_leaf and not feature.store:
                extractor.disconnect()
                del self[feature.key]

    def process(self, **kwargs):
        # get all root nodes (those that produce data, rather than consuming 
        # it)
        roots = self.roots()

        # ensure that kwargs contains *at least* the arguments needed for the
        # root nodes
        intersection = set(kwargs.keys()) & set(roots.keys())
        if len(intersection) < len(roots):
            raise KeyError(
                    (
                        'the keys {kw} were provided to the process() method, but the' \
                        + ' keys for the root extractors were {r}') \
                        .format(kw=kwargs.keys(), r=roots.keys()))

        graph_args = dict((k, kwargs[k]) for k in intersection)

        subscriptions = self.subscriptions()
        queue = deque()

        # get a generator for each root node.
        with contextlib.nested(*self.values()) as _:
            generators = [roots[k].process(v, queue=queue)
                          for k, v in graph_args.iteritems()]
            for _ in izip_longest(*generators):
                while queue:
                    key, fname, kwargs = queue.pop()
                    for subscriber in subscriptions[key]:
                        func = getattr(subscriber, fname)
                        try:
                            [_ for _ in func(**kwargs)]
                        except TypeError:
                            continue
