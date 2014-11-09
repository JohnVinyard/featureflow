

def chunked(f, chunksize = 4096):
    print chunksize
    data = f.read(chunksize)
    while data:
        yield data
        data = f.read(chunksize)