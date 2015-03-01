import os

def chunked(f, chunksize = 4096):
    data = f.read(chunksize)
    while data:
        yield data
        data = f.read(chunksize)

def ensure_path_exists(filename_or_directory):
    '''
    Given a filename, ensure that the path to it exists
    '''
    
    if not filename_or_directory:
        raise ValueError(\
                'filename_or_directory must be a path to a file or directory')
    
    parts = os.path.split(filename_or_directory)
    # find out if the last part has a file extension
    subparts = os.path.splitext(parts[-1])
    extension = subparts[-1]
    # we're only interested in creating directories, so leave off the last part,
    # if it's a filename
    parts = parts[:-1] if extension else parts
    path = os.path.join(*parts)
    
    
    if path:
        try: 
            os.makedirs(path)
        except OSError:
            # This probably means that the path already exists
            pass