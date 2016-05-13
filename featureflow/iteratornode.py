from extractor import Node


class IteratorNode(Node):
    def __init__(self, iterable=None, needs=None):
        self.iterable = iterable
        super(IteratorNode, self).__init__(needs=needs)

    def _process(self, data):
        for x in self.iterable:
            yield x
