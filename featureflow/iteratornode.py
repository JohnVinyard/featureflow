from extractor import Node


class IteratorNode(Node):
    def __init__(self, needs=None):
        super(IteratorNode, self).__init__(needs=needs)

    def _process(self, data):
        for x in data:
            yield x
