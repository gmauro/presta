"""Read version from file."""

import os

# https://docs.python.org/3/tutorial/modules.html#importing-from-a-package
__all__ = ['__version__']

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(os.path.dirname(here), 'VERSION')) as version_file:
    __version__ = version_file.read().strip()
