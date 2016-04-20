try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import os
import io
import re

def read(*names, **kwargs):
    with io.open(
        os.path.join(os.path.dirname(__file__), *names),
        encoding=kwargs.get("encoding", "utf8")
    ) as fp:
        return fp.read()

def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")

config = {
    'name': 'digSparkUtil',
    'description': 'digSparkUtil',
    'author': 'Andrew Philpot',
    'url': 'https://github.com/usc-isi-i2/dig-sparkutil',
    'download_url': 'https://github.com/usc-isi-i2/dig-sparkutil',
    'author_email': 'andrew.philpot@gmail.com',
    'install_requires': ['nose2'],
    # these are the (sub)modules of the current directory that we care about
    'packages': ['digSparkUtil'],
    'scripts': [],
    'version': find_version("digSparkUtil", "__init__.py")
}

setup(**config)
