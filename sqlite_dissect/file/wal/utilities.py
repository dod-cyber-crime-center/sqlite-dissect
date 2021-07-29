from logging import getLogger
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.file.database.header import DatabaseHeader

"""

utilities.py

This script holds utility functions for dealing with WAL specific objects such as comparing database header rather
than more general utility methods.

This script holds the following function(s):
compare_database_headers(previous_database_header, new_database_header)

"""


def compare_database_headers(previous_database_header, database_header):

    logger = getLogger(LOGGER_NAME)

    if not isinstance(previous_database_header, DatabaseHeader):
        log_message = "The previous database header is not a Database Header but has a type of: {}."
        log_message = log_message.format(type(previous_database_header))
        logger.error(log_message)
        raise ValueError(log_message)

    if not isinstance(database_header, DatabaseHeader):
        log_message = "The database header is not a Database Header but has a type of: {}."
        log_message = log_message.format(type(database_header))
        logger.error(log_message)
        raise ValueError(log_message)

    """

    Since the two objects are the same, we are not worried about possible differences in what properties the
    objects have.

    """

    database_header_changes = {}
    for key in previous_database_header.__dict__.keys():
        previous_value = getattr(previous_database_header, key)
        value = getattr(database_header, key)
        if previous_value != value:
            database_header_changes[key] = (previous_value, value)

    return database_header_changes
