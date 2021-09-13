from logging import getLogger
from re import match
from re import sub
from sqlite_dissect.constants import COLUMN_CONSTRAINT_PREFACES
from sqlite_dissect.constants import DATA_TYPE
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import TYPE_AFFINITY
from sqlite_dissect.exception import MasterSchemaRowParsingError
from sqlite_dissect.file.schema.utilities import get_index_of_closing_parenthesis

"""

column.py

This script holds the objects needed for parsing column related objects to the master schema.

This script holds the following object(s):
ColumnDefinition(object)

"""


class ColumnDefinition(object):

    def __init__(self, index, column_text, comments=None):

        logger = getLogger(LOGGER_NAME)

        self.index = index
        self.column_text = sub("\s\s+", " ", column_text.strip())

        """

        When the column text is sent in, the column text starts from the first column name until the "," in the
        following form:
        "COLUMN_NAME ... ,"

        Any comments that may appear before the COLUMN_NAME or after the "," should already be parsed and sent in
        through the constructor as the comments field.  However, there may still be comments in the column text
        itself, where the "...." appear above.  These are parsed out here and removing them from the column text.
        After the column text has all the comments removed, all multiple whitespace character segments including
        newlines, etc. are replaced by single whitespace characters and then the column text is stripped.  Comments
        are only stripped since the "-- ... \n" comment form cannot have more than the terminating "\n" character
        in it and the "/* ... */ segment may have "\n" characters in it for a reason, such as length of the comment.

        The way the comments are parsed out here is done by character and skipping ahead instead of pattern matches
        since technically a comment may have another comment form in it.

        Any comment pulled out from any place in the column definition is considered on the column definition level,
        and not tied to specific constraints, data types, etc.

        Note:  The self.column_text field will be set to the column text sent into this class with only whitespace
               modifications to strip the text and replace multiple whitespace characters with a single space, " ".

        """

        # Setup the field to parse the column text and comments
        parsed_column_text = ""
        parsed_comments = []
        parsed_comments_total_length = 0

        # Define an index for the parsing the column text
        character_index = 0

        # Iterate through all of the characters in the column text
        while character_index < len(column_text):

            # Get the current indexed character
            character = column_text[character_index]

            # Check for the "/* ... */" comment form
            if character == "/":
                last_comment_character_index = column_text.index("*/", character_index) + 1
                parsed_comment = column_text[character_index:last_comment_character_index + 1]
                parsed_comments_total_length += len(parsed_comment)
                parsed_comments.append(parsed_comment)
                character_index = last_comment_character_index

            # Check for the "-- ... \n" comment form
            elif character == "-" and column_text[character_index + 1] == "-":

                """

                Above, we check to make sure we are encountering a comment by checking the next character as well
                for the "-- ... \n" comment.

                Note:  A single "-" is allowed since it can be before a negative default value for example in the
                       create statement.

                """

                last_comment_character_index = column_text.index("\n", character_index)
                parsed_comment = column_text[character_index:last_comment_character_index + 1]
                parsed_comments_total_length += len(parsed_comment)
                parsed_comments.append(parsed_comment)
                character_index = last_comment_character_index

            else:
                parsed_column_text += character

            # Increment the character index
            character_index += 1

        # Make sure the parsed lengths add up correctly to the original length
        if parsed_comments_total_length + len(parsed_column_text) != len(column_text):
            log_message = "Column index: {} with column text: {} of length: {} was not parsed correctly.  The length " \
                          "of the parsed comments total length was: {} with the following comments: {} and the " \
                          "length of the parsed column text was: {} as: {}."
            log_message = log_message.format(self.index, column_text, len(column_text), parsed_comments_total_length,
                                             parsed_comments, len(parsed_column_text), parsed_column_text)
            logger.error(log_message)
            raise MasterSchemaRowParsingError(log_message)

        # Update the parsed column text replacing any whitespace with a single " " character and stripping it
        parsed_column_text = sub("\s\s+", " ", parsed_column_text.strip())

        # Check the comments sent in for validity
        if comments:
            for comment in comments:
                if not comment.startswith("--") and not comment.startswith("/*"):
                    log_message = "Comment specified does not start with the schema comment prefix: {}.".format(comment)
                    logger.error(log_message)
                    raise MasterSchemaRowParsingError(log_message)

        # Below we strip the comments but if a "\n" happens to be in a "/* ... */", we leave it alone.
        self.comments = [comment.strip() for comment in comments] if comments else []
        self.comments += [comment.strip() for comment in parsed_comments]

        # Retrieve the column name and remaining column text after the column name is removed
        self.column_name, \
            remaining_column_text = ColumnDefinition._get_column_name_and_remaining_sql(index, parsed_column_text)

        # Setup default values for the column definition fields
        self.derived_data_type_name = None
        self.data_type = DATA_TYPE.NOT_SPECIFIED
        self.column_constraints = []

        """

        If there is a remaining column text then we parse through it since there is either at least one data type
        or column constraint defined.

        There are 0..1 data types and 0...* column constraints if there is remaining column text.

        Note:  The following statements are valid column definitions:
               1.) field previous_field TEXT
               2.) field TEXT INTEGER BLOB

               This was noticed in a database that had a create table statement that had multiple field names but did
               not throw an error in SQLite.  This is because SQLite pulls the first field as the column name and then
               takes the string until it hits a column constraint as the whole data type field.  In the above examples,
               the derived data types would be:
               1.) previous_field TEXT
               2.) TEXT INTEGER BLOB

               SQLite checks for the data type seeing if certain patterns are in this string in a certain order (see
               the _get_column_affinity function for more information).  Therefore, the affinities of the two examples
               above would be:
               1.) TEXT
               2.) INTEGER

               Due to this, we parse out the data type the same way as SQLite.  We move through the file until we find
               a column constraint or the end of the column definition and then take that as the data type segment to
               check on.  Keep in mind there are more use cases that are tokenized during this process in SQLite.  For
               instance, if the column definition "field previous_field TEXT as BLOB" was specified, it would fail in
               SQLite since "as" is a word that is identified as a particular use case in addition to column
               constraints.  This will not be worried about here since this will address all use cases allowed by SQLite
               and be a superset of all of the use cases allowed for better compatibility instead of trying to handle
               all of the same token use cases in the SQLite library.

        """

        while len(remaining_column_text):

            # Get the next column definition segment
            segment_index = ColumnDefinition._get_next_segment_ending_index(self.index, self.column_name,
                                                                            remaining_column_text)

            # Make sure an error did not occur retrieving the segment index
            if segment_index <= 0 or segment_index > len(remaining_column_text):
                log_message = "Column name: {} with index: {} has a segment out of bounds with index: {} when the " \
                              "remaining column text is: {} with length: {} from full column text: {}."
                log_message = log_message.format(self.column_name, self.index, segment_index, remaining_column_text,
                                                 len(remaining_column_text), self.column_text)
                logger.error(log_message)
                raise IndexError(log_message)

            # Get the next segment
            segment = remaining_column_text[:segment_index + 1]

            if (len(segment) == len(remaining_column_text) or match("\w", remaining_column_text[segment_index + 1])) \
                and ColumnDefinition._is_column_constraint_preface(segment):

                """

                Here we set the column constraints to the rest of the remaining text.

                """

                # Set the column constraints
                self.column_constraints = [remaining_column_text]

                # Set the remaining column text  (This will be an empty string but needed to exit from while.)

                """

                The next step here is to parse the table constraints:
                remaining_column_text = remaining_column_text[len(self.column_constraints):]
                ...

                """

                break

            else:

                """

                The data type may have "(" and ")" characters in it to specify size (size of which is ignored by SQLite
                as a side note) and needs to be correctly accounted for.  Here we get rid of any whitespace around the
                parenthesis and then any leading or trailing whitespace.

                """

                segment = sub("\s*\(\s*", "(", segment)
                segment = sub("\s*\)\s*", ")", segment)
                segment = segment.strip()

                # Convert it to all uppercase for the derived data type name
                self.derived_data_type_name = segment.upper()

                # Obtain the data type (if possible, otherwise it will be INVALID) from the derived data type name
                self.data_type = self._get_data_type(self.derived_data_type_name)

                # Set the remaining column text accounting for the white space character after
                remaining_column_text = remaining_column_text[segment_index + 1:]

        self.type_affinity = self._get_column_affinity(self.data_type, self.derived_data_type_name)

    @staticmethod
    def _get_column_affinity(data_type, derived_data_type):

        column_type = data_type

        """
        
        Below we check if the data type was invalid.  If the data type is invalid, it means the original
        type statement was not a predefined type.  However, SQLite does not check against predefined types.
        The SQLite codes does string matches on what was defined to determine affinity.  For instance when
        defining a table: "CREATE TABLE example (a CHAR, b CHARACTER)", both a and b will be determined to have
        both TEXT affinity according to the rules below.  Due to this, we set the type to check on back to the
        derived data type since that has the original text in it with only some spacing modifications which is
        negligible.  Since the patterns are matched on case sensitivity, we call upper() on the derived data type.
        
        """

        if column_type == DATA_TYPE.INVALID:
            column_type = derived_data_type.upper()

        """
        
        In order to determine the column affinity from the declared column data type we have to follow the
        set of rules from the SQLite Data Type Documentation below in order:
        
        1.) If the declared type contains the string "INT" then it is assigned INTEGER affinity.
        2.) If the declared type of the column contains any of the strings "CHAR", "CLOB", or "TEXT"
            then that column has TEXT affinity. Notice that the type VARCHAR contains the string "CHAR" and is
            thus assigned TEXT affinity.
        3.) If the declared type for a column contains the string "BLOB" or if no type is specified then the column
            has affinity BLOB.
        4.) If the declared type for a column contains any of the strings "REAL", "FLOA", or "DOUB" then the column
            has REAL affinity.
        5.) Otherwise, the affinity is NUMERIC.
        
        """

        if "INT" in column_type:
            return TYPE_AFFINITY.INTEGER
        elif "CHAR" in column_type or "CLOB" in column_type or "TEXT" in column_type:
            return TYPE_AFFINITY.TEXT
        elif "BLOB" in column_type or column_type == DATA_TYPE.NOT_SPECIFIED:
            return TYPE_AFFINITY.BLOB
        elif "REAL" in column_type or "FLOA" in column_type or "DOUB" in column_type:
            return TYPE_AFFINITY.REAL
        else:
            return TYPE_AFFINITY.NUMERIC

    @staticmethod
    def _get_column_name_and_remaining_sql(index, column_text):

        # Initialize the logger
        logger = getLogger(LOGGER_NAME)

        """

        Since the column name can be in brackets, backticks, single quotes, or double quotes, we check to make sure
        the column name is not in brackets, backticks, single quotes, or double quotes.  If it is, our job is fairly
        simple, otherwise we parse it normally.

        Note:  SQLite allows backticks for compatibility with MySQL and allows brackets for compatibility with
               Microsoft databases.

        """

        if column_text[0] == "`":

            # The column name is surrounded by backticks
            match_object = match("^`(.*?)`", column_text)

            if not match_object:
                log_message = "No backtick match found for sql column definition: {} with text: {}."
                log_message = log_message.format(index, column_text)
                logger.error(log_message)
                raise MasterSchemaRowParsingError(log_message)

            # Set the column name and strip the backticks
            column_name = column_text[match_object.start():match_object.end()].strip("`")

            # Set the remaining column text
            remaining_column_text = column_text[match_object.end():]

            # Return the column name and remaining column text stripped of whitespace
            return column_name, remaining_column_text.strip()

        elif column_text[0] == "[":

            # The column name is surrounded by brackets
            match_object = match("^\[(.*?)\]", column_text)

            if not match_object:
                log_message = "No bracket match found for sql column definition: {} with text: {}."
                log_message = log_message.format(index, column_text)
                logger.error(log_message)
                raise MasterSchemaRowParsingError(log_message)

            # Set the column name and strip the brackets
            column_name = column_text[match_object.start():match_object.end()].strip("[]")

            # Set the remaining column text
            remaining_column_text = column_text[match_object.end():]

            # Return the column name and remaining column text stripped of whitespace
            return column_name, remaining_column_text.strip()

        elif column_text[0] == "\'":

            # The column name is surrounded by single quotes
            match_object = match("^\'(.*?)\'", column_text)

            if not match_object:
                log_message = "No single quote match found for sql column definition: {} with text: {}."
                log_message = log_message.format(index, column_text)
                logger.error(log_message)
                raise MasterSchemaRowParsingError(log_message)

            # Set the column name and strip the single quotes
            column_name = column_text[match_object.start():match_object.end()].strip("\'")

            # Set the remaining column text
            remaining_column_text = column_text[match_object.end():]

            # Return the column name and remaining column text stripped of whitespace
            return column_name, remaining_column_text.strip()

        elif column_text[0] == "\"":

            # The column name is surrounded by double quotes
            match_object = match("^\"(.*?)\"", column_text)

            if not match_object:
                log_message = "No double quote match found for sql column definition: {} with text: {}."
                log_message = log_message.format(index, column_text)
                logger.error(log_message)
                raise MasterSchemaRowParsingError(log_message)

            # Set the column name and strip the double quotes
            column_name = column_text[match_object.start():match_object.end()].strip("\"")

            # Set the remaining column text
            remaining_column_text = column_text[match_object.end():]

            # Return the column name and remaining column text stripped of whitespace
            return column_name, remaining_column_text.strip()

        else:

            """

            We know now that either the space character is used to separate the column name or the column name
            makes up the entirety of the column text if there is no space.

            """

            if column_text.find(" ") != -1:

                # There is whitespace delimiting the column name
                column_name = column_text[:column_text.index(" ")]

                # Parse the remaining column text
                remaining_column_text = column_text[column_text.index(" ") + 1:]

                # Return the column name and remaining column text stripped of whitespace
                return column_name, remaining_column_text.strip()

            else:

                # The whole column text is just the column name
                column_name = column_text

                # The remaining column text should be an empty string but we return it for better interoperability
                remaining_column_text = column_text[len(column_text):]

                if remaining_column_text:
                    log_message = "Column text remaining when none expected for column name: {} with text: {} " \
                                  "and remaining: {} for index: {}."
                    log_message = log_message.format(column_name, column_text, remaining_column_text, index)
                    logger.error(log_message)
                    raise MasterSchemaRowParsingError(log_message)

                # Return the column name and remaining column text stripped of whitespace
                return column_name, remaining_column_text.strip()

    @staticmethod
    def _get_data_type(derived_data_type):

        # Convert the derived data type to uppercase
        derived_data_type = derived_data_type.upper()

        # Remove any parenthesis along with numerical values
        derived_data_type = sub("\(.*\)$", "", derived_data_type)

        # Replace spaces with underscores
        derived_data_type = sub(" ", "_", derived_data_type)

        for data_type in DATA_TYPE:

            # We remove any numerical values from the end since sqlite does not recognize them in the data types
            if sub("_\d+.*$", "", data_type) == derived_data_type:
                return data_type

        # If no data type was found we return an invalid data type
        return DATA_TYPE.INVALID

    @staticmethod
    def _get_next_segment_ending_index(index, column_name, remaining_column_text):

        # Initialize the logger
        logger = getLogger(LOGGER_NAME)

        if len(remaining_column_text) == 0:
            log_message = "Invalid remaining column text of 0 length found for column index: {} with name: {}: {}."
            log_message = log_message.format(index, column_name, remaining_column_text)
            logger.error(log_message)
            raise ValueError(log_message)

        """

        Note:  We do not want to trim the string ourselves here since we are parsing text and do not know what the
               calling logic is doing outside this function.

        """

        # Make sure all space is trimmed from the front of the remaining column text as it should be
        if remaining_column_text[0].isspace():
            log_message = "Invalid remaining column text beginning with a space found for column " \
                          "index: {} with name: {}: {}."
            log_message = log_message.format(index, column_name, remaining_column_text)
            logger.error(log_message)
            raise ValueError(log_message)

        # Iterate through the remaining column text to find the next segment
        next_segment_ending_index = 0
        while next_segment_ending_index < len(remaining_column_text):

            """

            Note:  Since column constraints are not properly implemented at the moment the following will work for
                   column data types but in the future, when this is expanded for column constraints, the
                   constraints will all work the same way according to the documentation except for the FOREIGN KEY
                   constraint which has content following the closing parenthesis.

            """

            if remaining_column_text[next_segment_ending_index] == "(":

                # If we find a "(", we return the index of the closing ")" accounting for the following whitespace
                return get_index_of_closing_parenthesis(remaining_column_text, next_segment_ending_index) + 1

            elif remaining_column_text[next_segment_ending_index].isspace():

                if remaining_column_text[next_segment_ending_index + 1] == "(":

                    # If we find a "(", return the index of the closing one accounting for the following whitespace
                    return get_index_of_closing_parenthesis(remaining_column_text, next_segment_ending_index + 1) + 1

                """

                We do not have to worry about checking the length of the remaining column text since that is already
                done above.  However, this function does not properly check for constraint segments such as "DEFAULT 0"
                where there still may be content following the initial constraint.  However, constraints are not fully
                implemented at this time, and when this is returned it will be detected within this class, and the rest
                of the string will be used.  A TODO has been put at the top of this script in regards to this.

                Note:  We know that if there is a space, than there must be characters following that space since
                       all whitespace was replaced with single whitespaces and the string was trimmed.

                """

                if ColumnDefinition._is_column_constraint_preface(
                                                                remaining_column_text[next_segment_ending_index + 1:]):

                    return next_segment_ending_index

                else:
                    next_segment_ending_index += 1

            else:

                # Check if this segment index equals the end of the remaining column text and if so, return it

                if next_segment_ending_index + 1 == len(remaining_column_text):
                    return next_segment_ending_index

                next_segment_ending_index += 1

        """

        The next segment was unable to be found

        """

        log_message = "Was unable to find the next segment for column index: {} with name: {} on {}."
        log_message = log_message.format(index, column_name, remaining_column_text)
        logger.error(log_message)
        raise MasterSchemaRowParsingError(log_message)

    @staticmethod
    def _is_column_constraint_preface(segment):

        for column_constraint_preface in COLUMN_CONSTRAINT_PREFACES:

            """

            Note: When the check is done on the segment, we check the next character is not one of the allowed
                  characters in a column name, data type, etc. to make sure the constraint preface is not the 
                  beginning of a longer name where it is not actually a constraint preface (example: primaryEmail).
                  The "\w" regular expression when no LOCALE and UNICODE flags are set will be equivalent to the set:
                  [a-zA-Z0-9_].

            """

            # Check to see if the segment starts with the column constraint preface
            if segment.upper().startswith(column_constraint_preface):
                if not (len(column_constraint_preface) + 1 <= len(segment)
                        and match("\w", segment[len(column_constraint_preface)])):
                    return True

        return False

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding="", print_column_constraints=True):
        string = padding + "Column Text: {}\n" \
                 + padding + "Index: {}\n" \
                 + padding + "Column Name: {}\n" \
                 + padding + "Derived Data Type Name: {}\n" \
                 + padding + "Data Type: {}\n" \
                 + padding + "Type Affinity: {}\n" \
                 + padding + "Number of Comments: {}"
        string = string.format(self.column_text,
                               self.index,
                               self.column_name,
                               self.derived_data_type_name,
                               self.data_type,
                               self.type_affinity,
                               len(self.comments))
        for comment in self.comments:
            string += "\n" + padding + "Comment: {}".format(comment)
        if print_column_constraints:
            string += "\n" + padding + "Column Constraints: {}".format(self.column_constraints)
        return string


class ColumnConstraint(object):

    def __init__(self, index, constraint):

        self.index = index
        self.constraint = constraint

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding=""):
        string = padding + "Index: {}\n" \
                 + padding + "Constraint: {}"
        return string.format(self.index, self.constraint)
