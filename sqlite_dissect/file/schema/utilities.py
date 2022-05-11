from logging import getLogger
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.exception import MasterSchemaParsingError

"""

utilities.py

This script holds utility functions for dealing with schema specific objects such as parsing comments from sql rather
than more general utility methods.

This script holds the following function(s):
get_index_of_closing_parenthesis(string, opening_parenthesis_offset=0)
parse_comment_from_sql_segment(sql_segment)

"""


def get_index_of_closing_parenthesis(string, opening_parenthesis_offset=0):

    """


    Note:  Comments are skipped.

    Note:  The string to find the index of the closing parenthesis in requires there to be the opening parenthesis
           at the index of the opening parenthesis offset.  This can be 0 by default representing the opening
           parenthesis at the beginning of the string or a specified index.

    :param string: str  The string to find the index of the closing parenthesis in.
    :param opening_parenthesis_offset: int  The index of the first opening parenthesis.

    :return:

    :raise:

    """

    logger = getLogger(LOGGER_NAME)

    if string[opening_parenthesis_offset] != "(":
        log_message = "The opening parenthesis offset specifies a \"{}\" character and not the " \
                      "expected \"(\" in {} with opening parenthesis offset: {}."
        log_message = log_message.format(string[opening_parenthesis_offset], string, opening_parenthesis_offset)
        logger.error(log_message)
        raise ValueError(log_message)

    """

    We need to find the matching ")" character to the opening of the column definition area.  To
    do this we search looking for the ")" character but skip one for every matching "(" we find from the
    first occurrence.

    We also have to skip all comments indicated by "--" and "/*" and terminated by "\n" and "*/" respectively.
    In order to skip comments, we have to flag when we are in a comment.  In the case that we find:
    1.) "--" comment:  We set the comment_indicator field to 1 and back to 0 once the "\n" is found
    2.) "/*" comment:  We set the comment_indicator field to 2 and back to 0 once the "*/" is found

    Note:  If we are in a comment already, we ignore other comment indicators.

    """

    closing_parenthesis_offset = opening_parenthesis_offset
    embedded_parentheses = 0
    comment_indicator = 0
    literal_indicator = 0

    for index, character in enumerate(string[opening_parenthesis_offset + 1:], opening_parenthesis_offset + 1):

        closing_parenthesis_offset = index

        if comment_indicator:

            if (comment_indicator == 1 and character == '\n') or \
                    (comment_indicator == 2 and character == '/' and string[index - 1] == '*'):
                comment_indicator = 0

       	elif literal_indicator:
            if literal_indicator == 1 and character == '\'':
                literal_indicator = 0
            elif literal_indicator == 2 and character == '\"':
                literal_indicator = 0
            elif literal_indicator == 3 and character == '`':
                literal_indicator = 0

        else:

            if character == "(":

                embedded_parentheses += 1

            elif character == ")":

                if embedded_parentheses == 0:
                    break
                else:
                    embedded_parentheses -= 1

            elif character == "-":

                """

                Check to make sure we are encountering a comment.

                Note:  A single "-" is allowed since it can be before a negative default value for example in the
                       create statement.

                """

                # Check to make sure the full comment indicator was found for "--"
                if string[index + 1] == "-":

                    # Set the comment indicator
                    comment_indicator = 1

            elif character == "/":

                # Check to make sure the full comment indicators were found for "--" and "/*"
                if character == "/" and string[index + 1] != "*":
                    log_message = "Comment indicator '{}' found followed by an invalid secondary comment " \
                                  "indicator: {} found in {}."
                    log_message = log_message.format(character, string[index + 1], string)
                    logger.error(log_message)
                    raise MasterSchemaParsingError(log_message)

                # Set the comment indicator
                comment_indicator = 2

            elif character == "\'":
                literal_indicator = 1

            elif character == "\"":
                literal_indicator = 2

            elif character == "`":
                literal_indicator = 3

    # Check to make sure the closing parenthesis was found
    if closing_parenthesis_offset == len(string) - 1 and string[closing_parenthesis_offset] != ")":
        log_message = "The closing parenthesis was not found in {} with opening parenthesis offset: {}."
        log_message = log_message.format(string, opening_parenthesis_offset)
        logger.error(log_message)
        raise MasterSchemaParsingError(log_message)

    return closing_parenthesis_offset


def parse_comment_from_sql_segment(sql_segment):

    """

    This function will parse out the comment from the sql_segment.  This function assumes that a comment
    was already detected and needs to be parsed and therefore the sql_segment should start with:
    1.) --
    2.) /*

    If the sql_segment does not start with either, then an exception will be raised.  If a comment is
    found then the comment will be parsed out and returned along with the remaining sql.  Only the first comment
    will be stripped and returned in the case that there are multiple comments within the supplied sql_segment.

    If the either of the two above use cases above are found, then they will be parsed in the following manner:
    1.) --: The comment will be parsed from the "--" until the newline "\n" character is found:
        ... [-- ... \n] ...
    2.) /*: THe comment will be parsed from the "/*" until the matching "*/" character sequence is found:
        ... [/* ... */] ...
    Note:  The "/* ... */" comment tags can have new lines within them.

    Note:  The returned comment will include the "--" or "/* and "*/" strings.  If the comment was started with the
           "--" comment indicator, the ending '\n' character is included in the comment string.  It is up to the caller
           to call rstrip() or a likewise operation if needed.

    Note:  The returned remaining_sql_segment will not have strip() called on it.

    :param sql_segment:

    :return: tuple(comment, remaining_sql_segment)

    :raise: MasterSchemaParsingError

    """

    logger = getLogger(LOGGER_NAME)

    # Check if the sql segment starts with "--"
    if sql_segment.startswith("--"):

        comment = sql_segment[:sql_segment.index('\n') + 1]
        remaining_sql_segment = sql_segment[sql_segment.index('\n') + 1:]

        return comment, remaining_sql_segment

    # Check if the sql segment starts with "/*"
    elif sql_segment.startswith("/*"):

        comment = sql_segment[:sql_segment.index("*/") + 2]
        remaining_sql_segment = sql_segment[sql_segment.index("*/") + 2:]

        return comment, remaining_sql_segment

    # The remaining sql command does not start with "--" or "/*" as expected
    else:
        log_message = "The sql segment: {} did not start with the expected \"--\" or \"/*\" strings."
        log_message = log_message.format(sql_segment.number)
        logger.error(log_message)
        raise MasterSchemaParsingError(log_message)
