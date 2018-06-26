from itertools import izip_longest
import contextlib
from collections import deque, defaultdict
import inspect
from util import dictify
from hashlib import md5


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

        self._needs = dictify(needs)
        self._dependecy_names = dict(
            (id(v), k) for k, v in self._needs.iteritems())

        for n in self.dependencies:
            n.add_listener(self)

        self._finalized_dependencies = set()
        self._enqueued_dependencies = set()

    @property
    def dependencies(self):
        if isinstance(self._needs, dict):
            return self._needs.values()
        return self._needs

    def _dependency_name(self, pusher):
        return self._dependecy_names[id(pusher)]

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
        for e in self.dependencies:
            try:
                e._listeners.remove(self)
            except ValueError:
                pass

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

        if pusher in self.dependencies:
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


class FunctionalNode(Node):
    def __init__(self, func, needs=None, closure_fingerprint=None):
        """
        `FunctionalNode` makes it possible to specify a stateless processing
        node using a simple Python function (or callable).

        Args:
            func (callable): A callable to stand in for the `_process()` method
            needs (Node or List of Node): Processing nodes on which this one
                depends
            closure_fingerprint (callable): An optional callable that takes a
                dictionary of closed-over variable names and their values, and
                returns a string or buffer object that may be added to the hash
                that will constitute the version number
        """
        super(FunctionalNode, self).__init__(needs=needs)
        self.closure_fingerprint = closure_fingerprint
        self.func = func

    @property
    def version(self):
        """
        Compute the version identifier for this functional node using the
        func code and local names.  Optionally, also allow closed-over variable
        values to affect the version number when closure_fingerprint is
        specified
        """
        try:
            f = self.func.__call__.func_code
        except AttributeError:
            f = self.func.func_code

        h = md5()
        h.update(f.co_code)
        h.update(str(f.co_names))

        try:
            closure = self.func.__closure__
        except AttributeError:
            return h.hexdigest()

        if closure is None or self.closure_fingerprint is None:
            return h.hexdigest()

        d = dict(
            (name, cell.cell_contents)
            for name, cell in zip(f.co_freevars, closure))
        h.update(self.closure_fingerprint(d))

        return h.hexdigest()

    def _process(self, data):
        result = self.func(data)
        if inspect.isgenerator(result):
            for x in result:
                yield x
        else:
            yield result


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


class KeySelector(object):
    """
    A mixin for Node-derived classes that allows the extractor to process a
    single key from the dictionary-like object it is passed
    """

    def __init__(self, aspect_key, needs=None, **kwargs):
        super(KeySelector, self).__init__(needs=needs, **kwargs)
        self.aspect_key = aspect_key

    def _enqueue(self, data, pusher):
        try:
            # the data is dictionary-like, and contains the aspect key
            data = data[self.aspect_key]
        except TypeError:
            # the data is not a dictionary
            pass
        except KeyError:
            # the data is dictionary-like, but doesn't contain the aspect key
            pass

        super(KeySelector, self)._enqueue(data, pusher)


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
            for n in node.dependencies:
                subscriptions[id(n)].append(node)
        return subscriptions

    def remove_dead_nodes(self, features):
        # starting from the leaves, remove any nodes that are not stored, and 
        # have no stored consuming nodes
        mapping = dict((self[f.key], f) for f in features)
        nodes = deque(self.leaves().values())
        while nodes:
            extractor = nodes.pop()
            nodes.extendleft(extractor.dependencies)
            try:
                feature = mapping[extractor]
            except KeyError:
                continue
            if extractor.is_leaf and not feature.store:
                extractor.disconnect()
                try:
                    del self[feature.key]
                except KeyError:
                    pass

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
                        result = func(**kwargs)
                        if result is None:
                            continue
                        [_ for _ in result]
