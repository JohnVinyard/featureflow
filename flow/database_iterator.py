from extractor import Node


class DatabaseIterator(Node):
    def __init__(self, needs=None, func=None):
        super(DatabaseIterator, self).__init__(needs=needs)
        self._func = func

    def _process(self, data):
        for _id in data.iter_ids():
            try:
                yield self._func(_id)
            except:
                continue
