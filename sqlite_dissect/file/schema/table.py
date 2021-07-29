from logging import getLogger
from re import sub
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.exception import MasterSchemaRowParsingError

"""

table.py

This script holds the objects needed for parsing table related objects to the master schema.

This script holds the following object(s):
TableConstraint(object)

"""


class TableConstraint(object):

    def __init__(self, index, constraint, comments=None):

        logger = getLogger(LOGGER_NAME)

        self.index = index
        self.constraint = constraint

        if comments:
            for comment in comments:
                if not comment.startswith("--") or not comment.startswith("/*"):
                    log_message = "Comment specified does not start with the schema comment prefix: {}.".format(comment)
                    logger.error(log_message)
                    raise MasterSchemaRowParsingError(log_message)

        self.comments = [comment.strip() for comment in comments] if comments else []

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding=""):
        string = padding + "Index: {}\n" \
                 + padding + "Constraint: {}"
        for comment in self.comments:
            string += "\n" + padding + "Comment: {}".format(comment)
        return string.format(self.index, self.constraint)
