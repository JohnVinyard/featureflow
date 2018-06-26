[![Build Status](https://travis-ci.org/JohnVinyard/featureflow.svg?branch=master)](https://travis-ci.org/JohnVinyard/featureflow)
[![Coverage Status](https://coveralls.io/repos/github/JohnVinyard/featureflow/badge.svg?branch=master)](https://coveralls.io/github/JohnVinyard/featureflow?branch=master)
[![PyPI](https://img.shields.io/pypi/v/featureflow.svg)](https://pypi.python.org/pypi/featureflow)

# featureflow
featureflow is a python library that allows users to build feature extraction
pipelines in a declarative way, and control how and where those features are
persisted.

# Usage

The following example will compute word frequency in individual text documents,
and then over the entire corpus of documents, but featureflow isn't limited to
text data.  It's designed to work well with sequential/streaming data
(e.g. audio or video) that is often processed iteratively, in small chunks.

You can see [all the code in this example in one place here](https://github.com/JohnVinyard/featureflow/tree/master/examples/wordcount.py).

We can define a graph of processing nodes like this:

```python
import featureflow as ff


@ff.simple_in_memory_settings
class Document(ff.BaseModel):
    """
    Define the processing graph needed to extract document-level features,
    whether, and how those features should be persisted.
    """
    raw = ff.ByteStreamFeature(
        ff.ByteStream,
        chunksize=128,
        store=True)

    checksum = ff.JSONFeature(
        CheckSum,
        needs=raw,
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

We can define the individual processing "nodes" referenced in the graph above
like this:

```python
import featureflow as ff
from collections import Counter
import re
import hashlib

class Tokenizer(ff.Node):
    """
    Tokenize a stream of text into individual, normalized (lowercase)
    words/tokens
    """
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
        last_boundary = matches[-1].end()
        self._cache = self._cache[last_boundary:]
        return matches

    def _process(self, data):
        yield map(lambda x: x.groupdict()['word'].lower(), data)


class WordCount(ff.Aggregator, ff.Node):
    """
    Keep track of token frequency
    """
    def __init__(self, needs=None):
        super(WordCount, self).__init__(needs=needs)
        self._cache = Counter()

    def _enqueue(self, data, pusher):
        self._cache.update(data)


class CheckSum(ff.Aggregator, ff.Node):
    """
    Compute the checksum of a text stream
    """
    def __init__(self, needs=None):
        super(CheckSum, self).__init__(needs=needs)
        self._cache = hashlib.sha256()

    def _enqueue(self, data, pusher):
        self._cache.update(data)

    def _process(self, data):
        yield data.hexdigest()
```

We can also define a graph that will process an entire corpus of stored document
features:

```python
import featureflow as ff

@ff.simple_in_memory_settings
class Corpus(ff.BaseModel):
    """
    Define the processing graph needed to extract corpus-level features,
    whether, and how those features should be persisted.
    """
    docs = ff.Feature(
        lambda doc_cls: (doc.counts for doc in doc_cls),
        store=False)

    total_counts = ff.JSONFeature(
        WordCount,
        needs=docs,
        store=True)
```

Finally, we can execute these processing graphs and access the stored features
like this:

```python
from __future__ import print_function
import argparse

def process_urls(urls):
    for url in urls:
        Document.process(raw=url)


def summarize_document(doc):
    return 'doc {_id} with checksum {cs} contains "the" {n} times'.format(
            _id=doc._id,
            cs=doc.checksum,
            n=doc.counts.get('the', 0))


def process_corpus(document_cls):
    corpus_id = Corpus.process(docs=document_cls)
    return Corpus(corpus_id)


def summarize_corpus(corpus):
    return 'The entire text corpus contains "the" {n} times'.format(
        n=corpus.total_counts.get("the", 0))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--url',
        help='specify one or more urls of text files to ingest',
        required=True,
        action='append')
    args = parser.parse_args()

    process_urls(args.url)

    for doc in Document:
        print(summarize_document(doc))

    corpus = process_corpus(Document)
    print(summarize_corpus(corpus))
```

To see this in action we can:

```bash
python wordcount.py \
    --url http://textfiles.com/food/1st_aid.txt \
    --url http://textfiles.com/food/antibiot.txt \
    ...
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





