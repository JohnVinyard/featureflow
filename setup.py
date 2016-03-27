from setuptools import setup

try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except(IOError, ImportError):
    long_description = open('README.md').read()

version = '0.4'

download_url = 'https://github.com/jvinyard/featureflow/tarball/{version}'\
    .format(**locals())

setup(
        name='featureflow',
        version=version,
        url='https://github.com/JohnVinyard/featureflow',
        author='John Vinyard',
        author_email='john.vinyard@gmail.com',
        long_description=long_description,
        packages=['featureflow'],
        download_url=download_url,
        install_requires=['nose', 'unittest2', 'requests', 'lmdb']
)
