from collections import OrderedDict


def chunked(f, chunksize=4096):
    data = f.read(chunksize)
    while data:
        yield data
        data = f.read(chunksize)


def dictify(x, key_selector=lambda item: id(item)):
    if x is None:
        return OrderedDict()

    if isinstance(x, dict):
        return x

    try:
        return OrderedDict((key_selector(z), z) for z in x)
    except TypeError:
        return OrderedDict({key_selector(x): x})
