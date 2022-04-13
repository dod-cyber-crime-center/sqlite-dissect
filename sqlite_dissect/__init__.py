import logging
import warnings
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect._version import __version__

"""

__init__.py

This package will have scripts for overall usage throughout the SQLite Dissect library allowing the functionality
to parse through the data and access to underlying functions through an interface.

This init script will initialize the logger for this library with a NullHandler to prevent unexpected output
from applications that may not be implementing logging.  It will also ignore warnings reported by the python
warning by default.  (Warnings are also thrown to the logger when they occur in addition to the warnings
framework.)

Note:  This library will use warnings for things that may not be fully implemented or handled yet.  (In other cases,
       NotImplementedErrors may be raised.)  To turn off warnings use the "-W ignore" option.  See the Python
       documentation for further options.

"""


# Import interface as api
from sqlite_dissect.interface import *


def null_logger():
    try:

        # Import the NullHandler from the logging package
        from logging import NullHandler

    except ImportError:

        # Make our own if an error occurring while importing
        class NullHandler(logging.Handler):

            def emit(self, record):
                pass

    # Get the logger from the LOGGER_NAME constant and add the NullHandler to it
    logging.getLogger(LOGGER_NAME).addHandler(NullHandler())

    logging.getLogger(LOGGER_NAME).propagate = False

    # Ignore warnings by default
    warnings.filterwarnings("ignore")
