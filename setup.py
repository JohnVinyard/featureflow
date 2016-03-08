import os
from setuptools import setup


def read(fname):
    """
    This is yanked from the setuptools documentation at
    http://packages.python.org/an_example_pypi_project/setuptools.html. It is
    used to read the text from the README file.
    """
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='featureflow',
    version='0.1',
    url='https://github.com/JohnVinyard/featureflow',
    author='John Vinyard',
    author_email='john.vinyard@gmail.com',
    long_description=read('README.md'),
    packages=['featureflow'],
    download_url = 'https://github.com/jvinyard/featureflow/tarball/0.1',
    install_requires=['nose', 'unittest2', 'requests', 'lmdb']
)
