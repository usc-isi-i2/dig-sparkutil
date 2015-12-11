try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'name': 'digSparkUtil',
    'description': 'digSparkUtil',
    'author': 'Andrew Philpot',
    'url': 'https://github.com/usc-isi-i2/dig-sparkutil',
    'download_url': 'https://github.com/usc-isi-i2/dig-sparkutil',
    'author_email': 'andrew.philpot@gmail.com',
    'version': '0.1',
    'install_requires': ['nose2'],
    # these are the subdirs of the current directory that we care about
    'packages': ['digSparkUtil'],
    'scripts': [],
}

setup(**config)
