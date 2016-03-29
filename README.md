[![Build Status](https://travis-ci.org/JohnVinyard/featureflow.svg?branch=master)](https://travis-ci.org/JohnVinyard/featureflow)
[![Coverage Status](https://coveralls.io/repos/github/JohnVinyard/featureflow/badge.svg?branch=master)](https://coveralls.io/github/JohnVinyard/featureflow?branch=master)
[![PyPI](https://img.shields.io/pypi/v/featureflow.svg)](https://pypi.python.org/pypi/featureflow)

# featureflow
featureflow is a python library that allows users to build feature extraction pipelines in a declarative way, and store the results for later use

# Usage

The following example computes word frequency in a text document, but featureflow
isn't limited to text data.  It's designed to work well with sequential data 
(e.g. audio or video) that may be processed iteratively, in smaller chunks.

Given a simple feature-extraction pipeline like this one:

```python
import featureflow as ff

class Settings(ff.PersistenceSettings):
    id_provider = ff.UuidProvider()
    key_builder = ff.StringDelimitedKeyBuilder()
    database = ff.InMemoryDatabase(key_builder=key_builder)

class Document(ff.BaseModel, ff.PersistenceSettings):
    raw = ff.ByteStreamFeature(
            ff.ByteStream,
            chunksize=128,
            store=True)

    tokens = ff.Feature(
            Tokenizer,
            needs=raw,
            store=False)

    counts = ff.JSONFeature(
            WordCount,
            needs=tokens,
            store=True)
```

You can process a text document, and access the results:

```python
if __name__ == '__main__':
    _id = Document.process(raw='http://www.something.com/something.txt')
    doc = Document(_id)
    print '"workers" appears {workers} times'.format(**doc.counts)
```

Your code focuses on data transformations, and leaves orchestration and persistence
up to featureflow:

```python
import featureflow as ff

class Tokenizer(ff.Node):
    def __init__(self, needs=None):
        super(Tokenizer, self).__init__(needs=needs)
        self._cache = ''
        self._pattern = re.compile('(?P<word>[a-zA-Z]+)\W+')

    def _enqueue(self, data, pusher):
        self._cache += data

    def _dequeue(self):
        matches = list(self._pattern.finditer(self._cache))
        if not matches:
            raise ff.NotEnoughData()
        self._cache = self._cache[matches[-1].end():]
        return map(lambda x: x.groupdict()['word'].lower(), matches)

    def _process(self, data):
        yield data


class WordCount(ff.Aggregator, ff.Node):
    def __init__(self, needs=None):
        super(WordCount, self).__init__(needs=needs)
        self._cache = Counter()

    def _enqueue(self, data, pusher):
        self._cache.update(data)
```

# Installation

Python headers are required.  You can install by running:

```bash
apt-get install python-dev
```

Numpy is optional.  If you'd like to use it, the [Anaconda](https://www.continuum.io/downloads) distribution is highly recommended.

Finally, just

```bash
pip install featureflow
```





