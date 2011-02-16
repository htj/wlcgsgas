from distutils.core import setup

from wlcgsgas import __version__


setup(name='wlcgsgas',
      version=__version__,
      description='Library for creating and processing data from SGAS into WLCG format',
      author='Henrik Thostrup Jensen',
      author_email='htj@ndgf.org',
      url='http://www.ndgf.org/',
      packages=['wlcgsgas']
)

