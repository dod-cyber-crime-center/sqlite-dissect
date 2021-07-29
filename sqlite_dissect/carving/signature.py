from abc import ABCMeta
from abc import abstractmethod
from copy import copy
from logging import getLogger
from re import sub
from warnings import warn
from sqlite_dissect.carving.utilities import get_content_size
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import MASTER_SCHEMA_ROW_TYPE
from sqlite_dissect.constants import STORAGE_CLASS
from sqlite_dissect.constants import TYPE_AFFINITY
from sqlite_dissect.file.database.utilities import aggregate_leaf_cells
from sqlite_dissect.file.database.utilities import get_pages_from_b_tree_page
from sqlite_dissect.file.schema.master import OrdinaryTableRow
from sqlite_dissect.file.schema.master import VirtualTableRow
from sqlite_dissect.file.version_parser import VersionParser
from sqlite_dissect.exception import SignatureError

"""

signature.py

This script holds the objects for the signature generation of SQLite table and index b-trees for carving.

This script holds the following object(s):
Signature(VersionParser)
SchemaColumnSignature(object)
TableColumnSignature(object)
TableRowSignature(object)
ColumnSignature(object)
ColumnFixedLengthSignature(ColumnSignature)
ColumnVariableLengthSignature(ColumnSignature)
ColumnReducedVariableLengthSignature(ColumnVariableLengthSignature)
ColumnNonReducedVariableLengthSignature(ColumnVariableLengthSignature)

"""


class Signature(VersionParser):

    def __init__(self, version_history, master_schema_entry, version_number=None, ending_version_number=None):

        """



        Note:  The schema and table column signatures will be lists ordered in relation to the index of the column
               referred to in the table.  The table row signatures will be a dictionary indexed by the serial type
               signature from the record representing the unique combination of serial types for that row pointing
               to the related table row signature.

        Note:  The above note is not true for "without rowid" tables.  A warning will be raised if this
               case is encountered.

        Note:  It is important to pay attention to the column breakdown in the usage of this class in the case of an
               altered table.  This class leaves it up to the user to check for these fields and make use of them
               accordingly.

        :param version_history:
        :param master_schema_entry:
        :param version_number:
        :param ending_version_number:

        :return:

        :raise:

        """

        # Call to the super class
        super(Signature, self).__init__(version_history, master_schema_entry, version_number, ending_version_number)

        logger = getLogger(LOGGER_NAME)

        """

        Since the index signatures have not been fully investigated, a warning is printed here to alert of this.

        """

        if master_schema_entry.row_type == MASTER_SCHEMA_ROW_TYPE.INDEX:
            log_message = "An index row type was found for signature which is not fully supported for master " \
                          "schema entry root page number: {} row type: {} name: {} table name: {} and sql: {}."
            log_message = log_message.format(master_schema_entry.root_page_number,
                                             master_schema_entry.row_type, master_schema_entry.name,
                                             master_schema_entry.table_name, master_schema_entry.sql)
            logger.warn(log_message)
            warn(log_message, RuntimeWarning)

            if master_schema_entry.internal_schema_object:
                log_message = "An internal schema object index row type was found for the version parser which is " \
                              "not fully supported for master schema entry root page number: {} type: {} name: {} " \
                              "table name: {} and sql: {} and may result in erroneous cells."
                log_message = log_message.format(master_schema_entry.root_page_number,
                                                 master_schema_entry.row_type, master_schema_entry.name,
                                                 master_schema_entry.table_name, master_schema_entry.sql)
                logger.warn(log_message)
                warn(log_message, RuntimeWarning)

        log_message = "Creating signature for master schema entry with name: {} table name: {} row type: {} and " \
                      "sql: {} for version number: {} and ending version number: {}."
        log_message = log_message.format(self.name, self.table_name, self.row_type, self.sql,
                                         self.parser_starting_version_number, self.parser_ending_version_number)
        logger.debug(log_message)

        """

        Create and initialize the variables for the signature

        The schema column signatures and table column signatures will be in order that the fields are in the table.  The
        table row signatures will be in a dictionary keyed off of the record serial type signature.

        """

        self.schema_column_signatures = []
        self.table_row_signatures = {}
        self.table_column_signatures = []

        """

        Below variables are declared for total records and unique records.  These are counters to determine the number
        of total rows reviewed across all versions (including duplicates) and the unique rows (non-duplicated) between
        all versions.  This is due to the face that we can have multiple pages with the same data and only minor
        additions/subtractions to that data.  Therefore, total records will record the running total of all records
        regardless of uniqueness and unique records will be the total number of records with no duplicates included.

        Note:  We include the row id into the uniqueness.  This way similar signatures between different rows will
               build up a more accurate probability.

        """

        self.total_records = 0
        self.unique_records = 0

        """

        Derived the schema column signatures from the SQL statements in the master schema from the
        table and index types.

        Note:  The order of column definitions will match the columns as defined in the schema SQL statement.

        Note:  IndexRow master schema entries do not have column definitions at this time so we need to make sure
               the object is a OrdinaryTableRow object.  (VirtualTableRow objects or OrdinaryTableRow that are
               "without rowid" tables do not have column definitions at this time either.)  This results in only
               normal tables currently having signatures.  Warnings have already been thrown in regards to these 
               use cases above.

        """

        if isinstance(master_schema_entry, OrdinaryTableRow) and not master_schema_entry.without_row_id:
            for column_definition in master_schema_entry.column_definitions:
                self.schema_column_signatures.append(SchemaColumnSignature(column_definition))

        if isinstance(master_schema_entry, VirtualTableRow):

            """
            
            Below we initialize variables for the signature to prevent issues with the stringify method.  After that,
            a warning message is printed and the application continues on since the virtual tables in SQLite are not
            currently supported.  All fields are set to the defaults (False and/or None/Empty values).
            
            """
            self.altered_columns = False
            self.column_breakdown = {}

            log_message = "Virtual table found in signature for master schema entry with name: {} table name: {} " \
                          "row type: {} and sql: {} for version number: {} and ending version number: {}.  A " \
                          "signature will not be generated since virtual tables are not fully supported yet."
            log_message = log_message.format(self.name, self.table_name, self.row_type, self.sql,
                                             self.parser_starting_version_number, self.parser_ending_version_number)
            log_message = log_message.format()
            getLogger(LOGGER_NAME).warn(log_message)
            warn(log_message, RuntimeWarning)

        elif self.parser_starting_version_number is not None and self.parser_ending_version_number is not None:

            # Get the versions
            versions = version_history.versions

            """

            Below the column definitions are pulled from the initial, base version, master schema.  Since these columns
            will stay the same across all updates to the master schema entry, it is safe to set it here.  The only field
            that can be updated in the master schema entry without causing a new master schema entry is the root page
            number.

            """

            # Set the column definitions
            column_definitions = master_schema_entry.column_definitions

            # Create a set for account cells so we don't account for the same record twice across versions
            accounted_for_cell_digests = set()

            # Initialize the b-tree page numbers
            root_b_tree_page_numbers = []

            # Iterate through the versions in reference to this master schema entry
            for version_number in range(self.parser_starting_version_number,
                                        self.parser_ending_version_number + 1):

                version = versions[version_number]
                root_page_number = self.root_page_number_version_index[version_number]

                b_tree_updated = False

                # Check if this is the first version to be investigated
                if version_number == self.parser_starting_version_number:
                    b_tree_updated = True

                # Check if the root page number changed
                elif root_page_number != self.root_page_number_version_index[version_number - 1]:
                    b_tree_updated = True

                # Check if any of the non-root pages changed
                elif [page_number for page_number in root_b_tree_page_numbers
                      if page_number in version.updated_b_tree_page_numbers]:
                    b_tree_updated = True

                # Parse the b-tree page structure if it was updated
                if b_tree_updated:

                    # Get the root page and root page numbers from the first version
                    root_page = version.get_b_tree_root_page(root_page_number)
                    root_b_tree_page_numbers = [b_tree_page.number for b_tree_page
                                                in get_pages_from_b_tree_page(root_page)]

                    """

                    Below we aggregate the records together.  This function returns the total of records and then
                    a dictionary of records indexed by their cell md5 hex digest to record.  This dictionary may
                    hold less records than the total since records may have already been accounted for in previous
                    versions and are ignored since their cell md5 hex digests are in the accounted for cell digests
                    already.

                    Note:  The number of unique records reflects the total of all records in terms of uniqueness
                           regardless of the number of columns that are reflected in each row.

                    """

                    total, records = aggregate_leaf_cells(root_page, accounted_for_cell_digests, True)

                    # Add the totals to the counts
                    self.total_records += total
                    self.unique_records += len(records)

                    """

                    The column definitions in the master schema entry are parsed in order.  Therefore, the order of the
                    column definitions should be in the same order as the columns in the record.  These orders are
                    assumed to be equivalent to each other.

                    Note:  In SQLite, it is not possible to rename or remove columns, but columns can be added.
                           Therefore, some records may have less entries in then than the number of column definitions
                           and the table row signatures may have a different number of columns (lesser or equal to
                           the number of column definitions) in them.

                    """

                    # Iterate through each of the records
                    for cell_md5_hex_digest, record in records.iteritems():

                        """

                        Note:  The serial type signature is a series of serial types in a string to determine the
                               structure of that record.  For variable length columns, -2 is used for strings and
                               -1 is used for blobs.  The variable length signatures are similar to Epilog.

                        """

                        # Check if the serial type signature of the record is not already in the row signatures
                        if record.serial_type_signature not in self.table_row_signatures:

                            # Create and add a new table row signature
                            table_row_signature = TableRowSignature(column_definitions, record)
                            self.table_row_signatures[record.serial_type_signature] = table_row_signature

                        # The signature already exists
                        else:

                            # Update the table row signature
                            self.table_row_signatures[record.serial_type_signature].update(record)

            """

            Iterate through each of the table row signatures and update the total number of records that were parsed
            in order to create probability statistics for that row.

            We also track the count of each row and then match that against the accounted for records for additional
            validation.

            """

            total_table_row_signature_count = 0

            # Iterate through the table row signatures and set the total rows and increment the count
            for serial_type_signature, table_row_signature in self.table_row_signatures.iteritems():
                table_row_signature.number_of_rows = self.unique_records
                total_table_row_signature_count += table_row_signature.count

            # Make sure the count of records match
            if total_table_row_signature_count != self.unique_records:
                log_message = "The total table row signature count: {} does not match the number of unique " \
                              "records: {} for master schema entry row type: {} with root page number: {} name: {} " \
                              "table name: {} and sql: {}."
                log_message = log_message.format(total_table_row_signature_count, self.unique_records,
                                                 master_schema_entry.row_type, master_schema_entry.root_page_number,
                                                 master_schema_entry.name, master_schema_entry.table_name,
                                                 master_schema_entry.sql)
                logger.error(log_message)
                raise SignatureError(log_message)

            """

            Below we have to account for the use case of altered tables.

            In order to do this we have a altered columns boolean that is set to true if this is detected.  We also
            create a dictionary to represent the breakdown of the columns:

            column_breakdown[NUMBER_OF_COLUMNS] = (NUMBER_OF_ROWS, PROBABILITY)

            where NUMBER_OF_ROWS is the number of rows that has exactly the NUMBER_OF_COLUMNS in it, and
            where PROBABILITY is the NUMBER_OF_ROWS divided by the number of unique records.

            Additionally, there may be no entries in for the last modification to the table.  For example, there may be
            5 rows with 10 columns, but the latest SQL/schema for the table shows that it has 11 columns.  This can
            occur if no rows are inserted after the last alter statement.  In order to account for this, the number
            of columns found for the schema are checked against the column breakdown dictionary and if the number of
            columns is not found, it is added to the dictionary with 0 NUMBER_OF_ROWS and 0 PROBABILITY.  It is
            important to note that it is only added if the number of columns in the SQL/schema are greater than the
            number of columns in the row.  If the number of columns in the SQL/schema are less, than an exception
            will be raised.

            In the case that there are no entries in the table itself, the NUMBER_OF_ROWS and PROBABILITY will both
            be set to 0 for the SQL/schema number of columns in the column breakdown.

            It is up to the user of the signature class to check against the column breakdown in order to determine the
            best way to carve the data they are looking at.  This class merely supplies the information and leaves it up
            to the user on how to make use of it.

            Also, in regards to probability, the column signatures created have probability based off of the number of
            rows that column appeared in.  Therefore, columns added in later through alter table statements will have
            probability calculated based off of the number of rows that only had those columns in it.  In order to
            calculate the probability of a column signature across all rows, the probability of that signature should
            be multiplied by the probability that column shows up which can be derived through the column breakdown
            based off of it's column index.  A better way to do this may be able to be done moving forward.

            Note:  The altered columns flag is not 100% deterministic.  It can only be determined when:
                   1.) The number of columns are different lengths across rows
                   2.) The number of columns in the SQL/schema is greater than the number of columns in the rows

            Note:  It may be better to find a way to correlate the altered columns flag to a master schema associated
                   class.

            """

            # Instantiate the altered columns flag and the column breakdown
            self.altered_columns = False
            self.column_breakdown = {}

            # Iterate through all of the table row signatures and add up the counts of each one based on column count
            for table_row_signature in self.table_row_signatures.values():
                column_signature_length = len(table_row_signature.column_signatures)
                if column_signature_length in self.column_breakdown:
                    self.column_breakdown[column_signature_length] += table_row_signature.count
                else:
                    self.column_breakdown[column_signature_length] = table_row_signature.count

            # Get the number of columns in the schema and add it to the column breakdown if not already added
            schema_column_length = len(self.schema_column_signatures)
            if schema_column_length not in self.column_breakdown:
                self.column_breakdown[schema_column_length] = 0

            # Iterate through the column breakdown and compute probabilities
            for column_count in self.column_breakdown:
                row_count = self.column_breakdown[column_count]
                probability = float(row_count) / self.unique_records if self.unique_records else 0
                self.column_breakdown[column_count] = (row_count, probability)

            # The columns have been altered if there is more than one entry in the column breakdown
            if len(self.column_breakdown) > 1:
                self.altered_columns = True

            """

            At this point we have iterated through all the versions and found all of the table row signatures to each
            unique row structure that we found.  If there was no root page or no rows found in any of the pages, then
            the table row signatures will be empty.  Below we parse through each of the table row signatures and create
            column signatures across them inverting the data so we can see the signatures in two ways.  First, across
            the rows, and second, across the columns.

            """

            # Check if there were table row signatures found
            if self.table_row_signatures:

                """

                Next, we create a table row column dictionary with the column index as the key and the value an array
                of serial types aggregated across all of the table row signatures of that column index.  Once we get
                the table row column serial type arrays, we create the table column signatures.

                This process basically inverts the table row signatures in order to generate the table
                column signatures.

                Note:  The column definitions in the master schema entry are parsed in order.  Therefore, the order of
                       the column definitions should be in the same order as the columns in the record.  Also, since the
                       table row signatures are created off of the record columns and definitions the columns in the
                       table row signature will also be in the same order.  Previously, the column definition size was
                       used to iterate through each row with to get the columns pertaining to the column index of the
                       column definition.  However, every row may not have every column and therefore the length of the
                       column signatures for each row being iterated through is used.  This will occur if multiple
                       variations of columns occur in the row indicating a table that has been altered at some point.

                Note:  The indices of the column signatures should match the indices of the record columns and the
                       columns in the table row signatures since they are all derived originally from the master schema.
                       Below, the index in the range of column definitions size is used for the table row columns
                       creation and the column signatures in the table row signatures.

                """

                table_row_columns = {}

                # Iterate through the table row signatures and create the table row columns dictionary
                for table_row_md5_hex_digest, table_row_signature in self.table_row_signatures.iteritems():

                    # Iterate through all of the column signatures in the current table row signature
                    for column_index in range(len(table_row_signature.column_signatures)):

                        # Add or append the column signature in the table row columns dictionary
                        if column_index in table_row_columns:
                            table_row_columns[column_index].append(table_row_signature.column_signatures[column_index])
                        else:
                            table_row_columns[column_index] = [table_row_signature.column_signatures[column_index]]

                # Iterate through the table row columns and create the table column signatures
                for table_row_column_index, table_row_column_serial_type_array in table_row_columns.iteritems():
                    column_name = column_definitions[table_row_column_index].column_name
                    self.table_column_signatures.append(TableColumnSignature(table_row_column_index, column_name,
                                                                             table_row_column_serial_type_array))

            # No table row signatures were found
            else:

                """

                Note:  Both of these should be 0 if no table row signatures were found.  Checking the total records
                       should actually be enough for this check but both are checked for additional validity.

                """

                # Make sure no records were found
                if self.total_records or self.unique_records:
                    log_message = "The total records: {} and unique records: {} are both not 0 as expected for " \
                                  "master schema entry row type: {} with root page number: {} name: {} table " \
                                  "name: {} and sql: {}."
                    log_message = log_message.format(self.total_records, self.unique_records,
                                                     master_schema_entry.row_type, master_schema_entry.root_page_number,
                                                     master_schema_entry.name, master_schema_entry.table_name,
                                                     master_schema_entry.sql)
                    logger.error(log_message)
                    raise SignatureError(log_message)

            """

            At this point we now have two sets of signatures depending on the way you want to view the table signatures.
            1.) self._table_row_signatures: Each unique row of the table in relation to serial types with probability of
                                            each row and column serial type if it is a string or blob.
            2.) self._table_column_signatures: Each column of the table with the serial types realized across all the
                                               rows along with probability of each serial type in respect to that
                                               column.

            """

        """

        Since we may not have records, and may possibly not have a schema to parse schema column signatures from
        (depending if it is a virtual table, internal schema object, etc.), we check the lengths of the schema
        column signatures and table column signatures so that if both signatures exist, the column lengths must
        be equal.  We take the max of the two lengths as the number of columns.

        """

        schema_column_signatures_length = len(self.schema_column_signatures)
        table_column_signatures_length = len(self.table_column_signatures)

        if schema_column_signatures_length and table_column_signatures_length:
            if schema_column_signatures_length != table_column_signatures_length:
                log_message = "The schema column signatures length: {} is not equal to the table column signatures " \
                              "length: {} for master schema entry row type: {} with root page number: {} name: {} " \
                              "table name: {} and sql: {}."
                log_message = log_message.format(schema_column_signatures_length, table_column_signatures_length,
                                                 master_schema_entry.row_type, master_schema_entry.root_page_number,
                                                 master_schema_entry.name, master_schema_entry.table_name,
                                                 master_schema_entry.sql)
                logger.error(log_message)
                raise SignatureError(log_message)

        self.number_of_columns = max(schema_column_signatures_length, table_column_signatures_length)

    def stringify(self, padding="", print_table_row_signatures=True, print_schema_column_signatures=True,
                  print_table_column_signatures=True, print_column_signatures=True):
        string = "\n" \
                 + padding + "Number of Columns: {}\n" \
                 + padding + "Total Records: {}\n" \
                 + padding + "Unique Records: {}\n" \
                 + padding + "Altered Columns: {}\n" \
                 + padding + "Column Breakdown: {}\n" \
                 + padding + "Schema Column Signatures Length: {}\n" \
                 + padding + "Table Row Signatures Length: {}\n" \
                 + padding + "Table Column Signatures Length: {}\n" \
                 + padding + "Recommended Schema Column Signature: {}\n" \
                 + padding + "Complete Schema Column Signature: {}\n" \
                 + padding + "Focused Signature: {}\n" \
                 + padding + "Simplified Signature: {}\n" \
                 + padding + "Focused Probability Signature: {}\n" \
                 + padding + "Simplified Probability Signature: {}\n" \
                 + padding + "Epilog Schema Signature: {}\n" \
                 + padding + "Epilog Focused Signature: {}\n" \
                 + padding + "Epilog Simplified Signature: {}"
        string = string.format(self.number_of_columns,
                               self.total_records,
                               self.unique_records,
                               self.altered_columns,
                               self.column_breakdown,
                               len(self.schema_column_signatures),
                               len(self.table_row_signatures),
                               len(self.table_column_signatures),
                               self.recommended_schema_signature,
                               self.complete_schema_signature,
                               self.focused_signature,
                               self.simplified_signature,
                               self.focused_probabilistic_signature,
                               self.simplified_probabilistic_signature,
                               self.epilog_schema_signature,
                               self.epilog_focused_signature,
                               self.epilog_simplified_signature)
        if print_schema_column_signatures:
            for schema_column_signature in self.schema_column_signatures:
                signature_string = "\n" + padding + "Schema Column Signature: {}"
                signature_string = signature_string.format(schema_column_signature.stringify("\t"))
                string += signature_string
        if print_table_row_signatures:
            for table_row_md5_hex_digest, table_row_signature in self.table_row_signatures.iteritems():
                signature_string = "\n" + padding + "Table Row Signature:\n{}"
                signature_string = signature_string.format(table_row_signature.stringify("\t", print_column_signatures))
                string += signature_string
        if print_table_column_signatures:
            for table_column_signature in self.table_column_signatures:
                signature_string = "\n" + padding + "Table Column Signature: {}"
                signature_string = signature_string.format(table_column_signature.stringify("\t",
                                                                                            print_column_signatures))
                string += signature_string
        return super(Signature, self).stringify(padding) + string

    @property
    def epilog_focused_signature(self):

        epilog_focused_signature = []

        for column_signature in self.focused_signature:

            # Copy the column signature signature as a base
            epilog_column_signature = copy(column_signature)

            """

            Epilog does not log the 8 and 9 serial types in the focused schema.  Instead it uses serial type 1 for
            8 and 9.

            In order to represent 8 and 9 serial types in epilog column signatures, after epilog replaces the 8 or 9
            with a 1, it sets the min and max files appropriately for that field.  For example setting max = 1.

            More investigation needs to go into the use of epilog signatures with 8 and 9.

            """

            insert_single_byte_integer = False

            if 8 in epilog_column_signature:
                epilog_column_signature.remove(8)
                insert_single_byte_integer = True

            if 9 in epilog_column_signature:
                epilog_column_signature.remove(9)
                insert_single_byte_integer = True

            if insert_single_byte_integer and 1 not in epilog_column_signature:
                epilog_column_signature.append(1)

            epilog_focused_signature.append(sorted(epilog_column_signature, key=int))

        return epilog_focused_signature

    @property
    def epilog_schema_signature(self):

        epilog_schema_signature = []

        for schema_column_signature in self.schema_column_signatures:

            """

            Note:  The recommended signature is used here instead of the complete since this seems more in line
                   to the epilog signatures themselves, along with reducing a lot of serial types in the complete
                   signature that may not apply.

            """

            # Copy the recommended signature from this particular schema column signature as a base
            epilog_column_signature = copy(schema_column_signature.recommended_signature)

            # Append a null value as epilog does if it is not in the column signature already
            if 0 not in epilog_column_signature:
                epilog_column_signature.append(0)

            epilog_schema_signature.append(sorted(epilog_column_signature, key=int))

        return epilog_schema_signature

    @property
    def epilog_simplified_signature(self):

        epilog_simplified_signature = []

        for column_signature in self.simplified_signature:

            # Copy over the like serial types between this column signature and the epilog column signature
            epilog_column_signature = [x for x in column_signature if x in [-2, -1, 0, 7]]

            """

            Check if any of the integer serial types are in the column signature and add all integer serial
            types if any of them exist since this is how epilog seems to do it.  However, there may be use
            cases in regards to 8 and 9 being used for non-integer storage classes.

            """

            integer_serial_types = [1, 2, 3, 4, 5, 6, 8, 9]
            if len(set(integer_serial_types).intersection(set(column_signature))):
                epilog_column_signature.extend(integer_serial_types)

            epilog_simplified_signature.append(sorted(epilog_column_signature, key=int))

        return epilog_simplified_signature

    @property
    def complete_schema_signature(self):
        simplified_signatures = []
        for schema_column_signature in self.schema_column_signatures:
            simplified_signatures.append(schema_column_signature.complete_signature)
        return simplified_signatures

    @property
    def focused_probabilistic_signature(self):
        focused_signatures = []
        for table_column_signature in self.table_column_signatures:
            focused_signatures.append(table_column_signature.focused_probabilistic_signature)
        return focused_signatures

    @property
    def focused_signature(self):
        focused_signatures = []
        for table_column_signature in self.table_column_signatures:
            focused_signatures.append(table_column_signature.focused_signature)
        return focused_signatures

    @property
    def recommended_schema_signature(self):
        simplified_signatures = []
        for schema_column_signature in self.schema_column_signatures:
            simplified_signatures.append(schema_column_signature.recommended_signature)
        return simplified_signatures

    @property
    def simplified_probabilistic_signature(self):
        simplified_signatures = []
        for table_column_signature in self.table_column_signatures:
            simplified_signatures.append(table_column_signature.simplified_probabilistic_signature)
        return simplified_signatures

    @property
    def simplified_signature(self):
        simplified_signatures = []
        for table_column_signature in self.table_column_signatures:
            simplified_signatures.append(table_column_signature.simplified_signature)
        return simplified_signatures


class SchemaColumnSignature(object):

    """

    SchemaColumnSignature

    This class will take a column definition and create a schema column definition from it.  This is mostly useful
    in the case where there are not row entries in the table and a signature has to be built directly off the data
    types in the column definition.  Otherwise, the table column signature or table row signature would be recommended.
    This is due to the fact that this signature cannot validate the fields will be the types derived from the data types
    of the column due to the way SQLite works with storage classes and type affinities.  This class will retrieve the
    type affinity derived from the column data type (if specified) and base the signatures off of those affinities.
    Due to this, there will be two signatures in this class that can be retrieved:

    1.) Recommended Signature: The recommended signature for what is most likely to be seen in the columns based on the
                               type affinity.

        The recommended signature will be based off the data type and recommended storage class used for that data type,
        if specified.  The following serial types are used for the following type affinities:

        Type Affinity                   Serial Type Signature
        INTEGER                         [1, 2, 3, 4, 5, 6, 8, 9]
        REAL                            [1, 2, 3, 4, 5, 6, 7, 8, 9]
        NUMERIC                         [-2]
        TEXT                            [-1]
        BLOB (or if not specified)      [1, 2, 3, 4, 5, 6, 7, 8, 9]

    2.) Complete Signature: The full possibility of what can be seen in the columns based on the type affinity.

        Unfortunately, almost every type affinity can be stored as any storage class with the exception of the TEXT
        type affinity.  The storage class is derived from the combination of the type affinity and the actual value.
        Therefore the complete signature will include all storage classes for every type affinity except TEXT will
        will only include the TEXT, BLOB, and NULL storage classes.  (The TEXT, BLOB and NULL storage classes can be
        used for all type affinities.)

        Type Affinity                   Storage Class
        INTEGER                         INTEGER, REAL, TEXT, BLOB, NULL
        REAL                            INTEGER, REAL, TEXT, BLOB, NULL
        NUMERIC                         INTEGER, REAL, TEXT, BLOB, NULL
        TEXT                            TEXT, BLOB, NULL
        BLOB (or if not specified)      INTEGER, REAL, TEXT, BLOB, NULL

        Due to this, similar to above, there is also recommended storage class and possible storage class array for
        what the storage classes of the particular column may be.

        However, the REAL type affinity only uses the storage class INTEGER to store it's values into the file but
        reads it back out as REAL even though it is not in the file.  This conversion is done behind the scenes in
        SQLite and therefore the possible storage classes for REAL can be updated as:

        REAL                            REAL, TEXT, BLOB, NULL

        This is a very important (hidden) use case to keep in mind.

        This results in all type affinities having a signature of: [-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9], instead
        of the TEXT type affinity which has a signature of: [-2, -1, 0].

    Since many storage classes are possible for each data type, the possible storage classes are set in an array and
    are as specified above.

    Note:  Serial types 8 and 9 are used in all recommended signatures (except TEXT) since these two types are for 0 and
           1 constants which are used a lot in order to reserve space in the SQLite file.

    Note:  In the column definition, the derived data type name may be None if no data type was specified in the
           SQL.  If this is the case, the data type will be invalid and the type affinity will be BLOB per the
           way affinities and storage classes are related depending on data type to the SQLite documentation.

    Note:  Since TEXT and BLOB are variable length data types, -1 will be used to represent a BLOB and -2 will be used
           to represent a string.  This is similar to Epilog's handling of variable length data types in signatures.

    Note:  There may be the possibility that columns were added causing inconsistencies between previous versions of the
           row data that may not be picked up if solely going off of a schema based signature.  However, if there is no
           data to derive a signature from, we have no other recourse but to use the schema signature.  In the future
           signature files may be able to be imported in and out for this purpose based on os, application, and version.

    """

    def __init__(self, column_definition):

        self.derived_data_type_name = column_definition.derived_data_type_name
        self.data_type = column_definition.data_type
        self.type_affinity = column_definition.type_affinity

        if self.type_affinity == TYPE_AFFINITY.INTEGER:

            self.recommended_storage_class = STORAGE_CLASS.INTEGER
            self.possible_storage_classes = [STORAGE_CLASS.INTEGER, STORAGE_CLASS.REAL, STORAGE_CLASS.TEXT,
                                             STORAGE_CLASS.BLOB, STORAGE_CLASS.NULL]

            self.recommended_signature = [1, 2, 3, 4, 5, 6, 8, 9]
            self.complete_signature = [-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

        elif self.type_affinity == TYPE_AFFINITY.REAL:

            self.recommended_storage_class = STORAGE_CLASS.REAL
            self.possible_storage_classes = [STORAGE_CLASS.REAL, STORAGE_CLASS.TEXT,
                                             STORAGE_CLASS.BLOB, STORAGE_CLASS.NULL]

            self.recommended_signature = [1, 2, 3, 4, 5, 6, 7, 8, 9]
            self.complete_signature = [-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

        elif self.type_affinity == TYPE_AFFINITY.TEXT:

            self.recommended_storage_class = TYPE_AFFINITY.TEXT
            self.possible_storage_classes = [STORAGE_CLASS.TEXT, STORAGE_CLASS.BLOB, STORAGE_CLASS.NULL]

            self.recommended_signature = [-2]
            self.complete_signature = [-2, -1, 0]

        elif self.type_affinity == TYPE_AFFINITY.BLOB:

            self.recommended_storage_class = TYPE_AFFINITY.BLOB
            self.possible_storage_classes = [STORAGE_CLASS.INTEGER, STORAGE_CLASS.REAL, STORAGE_CLASS.TEXT,
                                             STORAGE_CLASS.BLOB, STORAGE_CLASS.NULL]

            self.recommended_signature = [-1]
            self.complete_signature = [-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

        elif self.type_affinity == TYPE_AFFINITY.NUMERIC:

            self.recommended_storage_class = TYPE_AFFINITY.NUMERIC
            self.possible_storage_classes = [STORAGE_CLASS.INTEGER, STORAGE_CLASS.REAL, STORAGE_CLASS.TEXT,
                                             STORAGE_CLASS.BLOB, STORAGE_CLASS.NULL]

            self.recommended_signature = [1, 2, 3, 4, 5, 6, 7, 8, 9]
            self.complete_signature = [-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

        else:

            log_message = "Invalid type affinity found: {}.".format(self.type_affinity)
            getLogger(LOGGER_NAME).error(log_message)
            raise SignatureError(log_message)

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding=""):
        string = padding + "Derived Data Type Name: {}\n" \
                 + padding + "Data Type: {}\n" \
                 + padding + "Type Affinity: {}\n" \
                 + padding + "Recommended Storage Class: {}\n" \
                 + padding + "Possible Storage Classes: {}\n" \
                 + padding + "Recommended Signature: {}\n" \
                 + padding + "Complete Signature: {}"
        string = string.format(self.derived_data_type_name,
                               self.data_type,
                               self.type_affinity,
                               self.recommended_storage_class,
                               self.possible_storage_classes,
                               self.recommended_signature,
                               self.complete_signature)
        return string


class TableColumnSignature(object):

    def __init__(self, index, name, column_signatures):

        self._logger = getLogger(LOGGER_NAME)

        self.count = 0
        self.index = index
        self.name = name
        self.column_signatures = {}

        for column_signature in column_signatures:

            if column_signature.index != self.index:
                log_message = "Invalid column signature index: {} found for table column signature with index: {} " \
                              "and name: {}."
                log_message = log_message.format(column_signature.index, self.index, self.name)
                self._logger.error(log_message)
                raise SignatureError(log_message)

            if column_signature.name != self.name:
                log_message = "Invalid column signature name: {} found for table column signature with name: {} " \
                              "and name: {}."
                log_message = log_message.format(column_signature.name, self.index, self.name)
                self._logger.error(log_message)
                raise SignatureError(log_message)

            self.count += column_signature.count

            if column_signature.serial_type in self.column_signatures:

                if isinstance(column_signature, ColumnFixedLengthSignature):
                    updated_column_signature = self.column_signatures[column_signature.serial_type]
                    updated_column_signature.update(column_signature.serial_type, column_signature.count)

                elif isinstance(column_signature, ColumnVariableLengthSignature):
                    updated_column_signature = self.column_signatures[column_signature.serial_type]
                    updated_column_signature.update(column_signature.serial_type, column_signature.count,
                                                    column_signature.variable_length_serial_types)

                else:
                    log_message = "Invalid column signature type: {} found for table column signature with index: {} " \
                                  "and name: {}."
                    log_message = log_message.format(type(column_signature), self.index, self.name)
                    self._logger.error(log_message)
                    raise SignatureError(log_message)

            else:

                if isinstance(column_signature, ColumnFixedLengthSignature):
                    new_column_signature = ColumnFixedLengthSignature(index, column_signature.name,
                                                                      column_signature.serial_type,
                                                                      column_signature.count)
                    self.column_signatures[column_signature.serial_type] = new_column_signature

                elif isinstance(column_signature, ColumnVariableLengthSignature):
                    new_column_signature = ColumnReducedVariableLengthSignature(index, column_signature.name,
                                                                                column_signature.serial_type,
                                                                                column_signature.count,
                                                                                column_signature.
                                                                                variable_length_serial_types)
                    self.column_signatures[column_signature.serial_type] = new_column_signature

                else:
                    log_message = "Invalid column signature type: {} found for table column signature with index: {} " \
                                  "and name: {}."
                    log_message = log_message.format(type(column_signature), self.index, self.name)
                    self._logger.error(log_message)
                    raise SignatureError(log_message)

        for column_signature_index, column_signature in self.column_signatures.iteritems():
            column_signature.number_of_rows = self.count

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding="", print_column_signatures=True):
        string = padding + "Index: {}\n" \
                 + padding + "Name: {}\n" \
                 + padding + "Count: {}\n" \
                 + padding + "Focused Signature: {}\n" \
                 + padding + "Simple Signature: {}\n" \
                 + padding + "Column Signature Length: {}"
        string = string.format(self.index,
                               self.name,
                               self.count,
                               self.focused_signature,
                               self.simplified_signature,
                               len(self.column_signatures))
        if print_column_signatures:
            for column_signature_index, column_signature in self.column_signatures.iteritems():
                string += "\n" + padding + "Column Signature:\n{}".format(column_signature.stringify(padding + "\t"))
        return string

    @property
    def focused_probabilistic_signature(self):
        focused_signatures = []
        for column_signature_index, column_signature in self.column_signatures.iteritems():
            if isinstance(column_signature, ColumnVariableLengthSignature):
                for serial_type in column_signature.variable_length_serial_types:
                    serial_type_probability = column_signature.get_variable_length_serial_type_probability(serial_type)
                    focused_signatures.append((serial_type, serial_type_probability))
            elif isinstance(column_signature, ColumnFixedLengthSignature):
                focused_signatures.append((column_signature.serial_type, column_signature.probability))
            else:
                log_message = "Invalid column signature type: {} found for table column signature with index: {} " \
                              "and name: {}."
                log_message = log_message.format(type(column_signature), self.index, self.name)
                self._logger.error(log_message)
                raise ValueError(log_message)
        return sorted(focused_signatures, key=lambda x: x[0])

    @property
    def focused_signature(self):
        focused_signatures = []
        for column_signature_index, column_signature in self.column_signatures.iteritems():
            if isinstance(column_signature, ColumnVariableLengthSignature):
                focused_signatures.extend(column_signature.variable_length_serial_types.keys())
            elif isinstance(column_signature, ColumnFixedLengthSignature):
                focused_signatures.append(column_signature.serial_type)
            else:
                log_message = "Invalid column signature type: {} found for table column signature with index: {} " \
                              "and name: {}."
                log_message = log_message.format(type(column_signature), self.index, self.name)
                self._logger.error(log_message)
                raise ValueError(log_message)
        return sorted(focused_signatures, key=int)

    @property
    def simplified_probabilistic_signature(self):
        simplified_signatures = []
        for column_signature_index, column_signature in self.column_signatures.iteritems():
            simplified_signatures.append((column_signature.serial_type, column_signature.probability))
        return sorted(simplified_signatures, key=lambda x: x[0])

    @property
    def simplified_signature(self):
        simplified_signatures = []
        for column_signature_index, column_signature in self.column_signatures.iteritems():
            simplified_signatures.append(column_signature.serial_type)
        return sorted(simplified_signatures, key=int)


class TableRowSignature(object):

    """

    TableRowSignature

    This class represents a signature of a particular row in a table.  The idea is that each table has similar rows
    in respect to their serial type ordering (storage classes and type affinities).  A array is made of these
    representing all signatures in a table and then can be inverted to represent the column signatures of a table.

    Note:  The number of columns in a table row signature may be equal to or less than the number of column definitions
           since columns can be added over time.  However, columns cannot be removed or renamed in SQLite.

    Note:  ColumnFixedLengthSignature column signatures will always have a probability of 1 in table row signatures,
           since this is identifying a unique combination of column signatures (serial types).  The
           ColumnVariableLengthSignature column signatures will have a similar probability of 1 in reference to TEXT
           and BLOB storage classes but may differ in the variable lengths themselves.  Due to this, there is no
           probabilistic signatures for table row signatures as there are in table column signatures.

    """

    def __init__(self, column_definitions, record):

        """

        Constructor.

        Note:  Table row signatures are determined from the record serial type signature.  Rows with the same serial
               type signature for records will be grouped into individual table row signatures and "counted".

        Note:  The column definitions array and the record columns in the record are relative to each other in terms
               of order since the column definitions are pulled from the master schema.

        :param column_definitions:
        :param record:

        :return:

        """

        self._logger = getLogger(LOGGER_NAME)

        # Get the record columns
        record_columns = record.record_columns

        self.count = 1
        self.column_signatures = {}
        self.record_serial_type_signature = record.serial_type_signature

        """

        Below we check to make sure the number of record column for this table row signature are less than or equal to
        the number of column definitions.  Since columns can be added, but not removed or renamed, the number of record
        columns can be less than the number of column definitions.  However, added columns are always appended to the
        table and therefore the column definitions will align up to the number of record columns that are found.

        We raise an exception if we find that the number of record columns is greater than the number of column
        definitions.  If we find that the record columns is less than the number of column definitions, we print
        a debug message.

        """

        # Check the length of the column definitions to the record columns
        if len(column_definitions) != len(record_columns):

            # Check if the column definitions is less than the number of record columns
            if len(column_definitions) < len(record_columns):
                log_message = "The length of column definitions: {} is less than the record column length: {} " \
                              "for table row signature with record serial type signature: {}."
                log_message = log_message.format(len(column_definitions), len(record_columns),
                                                 self.record_serial_type_signature)
                self._logger.error(log_message)
                raise ValueError(log_message)

            # The number of column definitions is greater than the number of record columns
            else:
                log_message = "The length of column definitions: {} is greater than the record column length: {} " \
                              "for table row signature with record serial type signature: {}."
                log_message = log_message.format(len(column_definitions), len(record_columns),
                                                 self.record_serial_type_signature)
                self._logger.debug(log_message)

        """

        Note:  The count is the number of specific rows that were found with this serial type whereas the number of
               rows is the total of the rows in the table this column signature is being derived from.  Therefore,
               the probability of this column signature with this serial type occurring in the particular column of
               the table is the count/total.

        """

        self._number_of_rows = None

        for index in range(len(record_columns)):

            column_name = column_definitions[index].column_name
            serial_type = record_columns[index].serial_type

            if 0 <= serial_type <= 9:
                self.column_signatures[index] = ColumnFixedLengthSignature(index, column_name, serial_type)
            elif serial_type >= 12:
                self.column_signatures[index] = ColumnNonReducedVariableLengthSignature(index, column_name, serial_type)
            else:
                log_message = "Invalid serial type: {} for table row signature with record serial type signature: {}."
                log_message = log_message.format(serial_type, self.record_serial_type_signature)
                self._logger.error(log_message)
                raise SignatureError(log_message)

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding="", print_column_signatures=True):
        string = padding + "Record Serial Type Signature: {}\n" \
                 + padding + "Count: {}\n" \
                 + padding + "Number of Rows: {}\n" \
                 + padding + "Probability: {}\n" \
                 + padding + "Focused Signature: {}\n" \
                 + padding + "Simple Signature: {}\n" \
                 + padding + "Column Signature Length: {}"
        string = string.format(self.record_serial_type_signature,
                               self.count,
                               self.number_of_rows,
                               self.probability,
                               self.focused_signature,
                               self.simplified_signature,
                               len(self.column_signatures))
        if print_column_signatures:
            for column_signature_index, column_signature in self.column_signatures.iteritems():
                string += "\n" + padding + "Column Signature:\n{}".format(column_signature.stringify(padding + "\t"))
        return string

    @property
    def focused_signature(self):
        focused_signatures = []
        for column_signature_index, column_signature in self.column_signatures.iteritems():
            if isinstance(column_signature, ColumnVariableLengthSignature):
                focused_signatures.append(sorted(column_signature.variable_length_serial_types.keys(), key=int))
            elif isinstance(column_signature, ColumnFixedLengthSignature):
                focused_signatures.append([column_signature.serial_type])
            else:
                log_message = "Invalid column signature type: {} found for table row signature with record serial " \
                              "type signature: {}."
                log_message = log_message.format(type(column_signature), self.record_serial_type_signature)
                self._logger.error(log_message)
                raise ValueError(log_message)
        return focused_signatures

    @property
    def number_of_rows(self):

        """



        Note:  A value of None will be returned if the number of rows is not set.

        :return:

        """

        return self._number_of_rows

    @number_of_rows.setter
    def number_of_rows(self, number_of_rows):

        if number_of_rows <= 0 or number_of_rows < self.count:
            log_message = "Invalid number of rows: {} for table row signature with record serial type signature: {}."
            log_message = log_message.format(number_of_rows, self.record_serial_type_signature)
            self._logger.error(log_message)
            raise ValueError(log_message)

        self._number_of_rows = number_of_rows

        for column_signature_index, column_signature in self.column_signatures.iteritems():
            column_signature.number_of_rows = number_of_rows

    @property
    def probability(self):

        """



        Note:  A value of None will be returned if the number of rows is not set.

        :return:

        """

        if self._number_of_rows:
            return float(self.count) / self._number_of_rows
        return None

    @property
    def simplified_signature(self):
        simplified_signatures = []
        for column_signature_index, column_signature in self.column_signatures.iteritems():
            simplified_signatures.append([column_signature.serial_type])
        return simplified_signatures

    def update(self, record):

        self.count += 1

        record_columns = record.record_columns

        # Check the length of each (we assume the order in relative to each other is the same)
        if len(self.column_signatures) != len(record_columns):
            log_message = "The length of column signatures: {} does not match record column length from record: {} " \
                          "for table row signature with record serial type signature: {}."
            log_message = log_message.format(len(self.column_signatures), len(record_columns),
                                             self.record_serial_type_signature)
            self._logger.error(log_message)
            raise ValueError(log_message)

        for index in self.column_signatures:

            serial_type = record_columns[index].serial_type
            column_signature = self.column_signatures[index]

            if isinstance(column_signature, ColumnFixedLengthSignature):

                if column_signature.serial_type != serial_type:
                    log_message = "Column signature serial type: {} does not match record serial type: {} " \
                                  "for table row signature with record serial type signature: {}."
                    log_message = log_message.format(column_signature.serial_type, serial_type,
                                                     self.record_serial_type_signature)
                    self._logger.error(log_message)
                    raise SignatureError(log_message)

                column_signature.update(serial_type)

            elif isinstance(column_signature, ColumnVariableLengthSignature):

                if serial_type >= 12 and serial_type % 2 == 0:
                    if column_signature.serial_type != -1:
                        log_message = "Column signature serial type: {} does not equate to record column variable " \
                                      "length serial type: {} for table row signature with record serial " \
                                      "type signature: {}."
                        log_message = log_message.format(column_signature.serial_type, serial_type,
                                                         self.record_serial_type_signature)
                        self._logger.error(log_message)
                        raise SignatureError(log_message)

                elif serial_type >= 13 and serial_type % 2 == 1:
                    if column_signature.serial_type != -2:
                        log_message = "Column signature serial type: {} does not equate to record column variable " \
                                      "length serial type: {} for table row signature with record serial " \
                                      "type signature: {}."
                        log_message = log_message.format(column_signature.serial_type, serial_type,
                                                         self.record_serial_type_signature)
                        self._logger.error(log_message)
                        raise SignatureError(log_message)

                else:
                    log_message = "Invalid serial type: {} for column variable length signature " \
                                  "for table row signature with record serial type signature: {}."
                    log_message = log_message.format(serial_type, self.record_serial_type_signature)
                    self._logger.error(log_message)
                    raise SignatureError(log_message)

                column_signature.update(serial_type)

            else:

                log_message = "Invalid column signature type: {} found for table row signature with record serial " \
                              "type signature: {}."
                log_message = log_message.format(type(column_signature), self.record_serial_type_signature)
                self._logger.error(log_message)
                raise SignatureError(log_message)


class ColumnSignature(object):

    __metaclass__ = ABCMeta

    def __init__(self, index, name, serial_type, count=1):

        """

        Constructor.

        Note:  All columns within a signature may have different counts.  This is due to the fact that columns can
               be added in SQLite.  If this occurs then columns towards the end of the rows may have less entries
               (if any) than previous column counts.

        :param index:
        :param name:
        :param serial_type:
        :param count:

        """

        self._logger = getLogger(LOGGER_NAME)

        self.index = index
        self.name = name
        self.serial_type = serial_type
        self.count = count

        """

        Note:  The count is the number of specific rows that were found with this serial type whereas the number of
               rows is the total of the rows in the table this column signature is being derived from.  Therefore,
               the probability of this column signature with this serial type occurring in the particular column of
               the table is the count/total.

        """

        self._number_of_rows = None

        # These values are reserved and should not be found in SQLite files
        if self.serial_type == 10 or self.serial_type == 11:
            log_message = "Invalid serial type: {} found for column signature index: {} and name: {}."
            log_message = log_message.format(self.serial_type, self.index, self.name)
            self._logger.error(log_message)
            raise ValueError(log_message)

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding=""):
        string = padding + "Index: {}\n" \
                 + padding + "Name: {}\n" \
                 + padding + "Serial Type: {}\n" \
                 + padding + "Count: {}\n" \
                 + padding + "Number of Rows: {}\n" \
                 + padding + "Probability: {}"
        return string.format(self.index,
                             self.name,
                             self.serial_type,
                             self.count,
                             self.number_of_rows,
                             self.probability)

    @property
    def number_of_rows(self):

        """



        Note:  A value of None will be returned if the number of rows is not set.

        :return:

        """

        return self._number_of_rows

    @number_of_rows.setter
    def number_of_rows(self, number_of_rows):
        if number_of_rows <= 0 or number_of_rows < self.count:
            log_message = "Invalid number of rows: {} for column signature index: {} and name: {}"
            log_message = log_message.format(number_of_rows, self.index, self.name)
            self._logger.error(log_message)
            raise ValueError(log_message)
        self._number_of_rows = number_of_rows

    @property
    def probability(self):

        """



        Note:  A value of None will be returned if the number of rows is not set.

        :return:

        """

        if self._number_of_rows:
            return float(self.count) / self._number_of_rows
        return None

    @abstractmethod
    def update(self, serial_type, count=None, variable_length_serial_types=None):
        raise NotImplementedError("The abstract method update was called directly and is not implemented.")


class ColumnFixedLengthSignature(ColumnSignature):

    def __init__(self, index, name, serial_type, count=1):

        super(ColumnFixedLengthSignature, self).__init__(index, name, serial_type, count)

        if serial_type not in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]:
            log_message = "Invalid serial type for column fixed-length signature index: {} and name: {}"
            log_message = log_message.format(serial_type, self.index, self.name)
            self._logger.error(log_message)
            raise ValueError(log_message)

        self.content_size = get_content_size(self.serial_type)

    def stringify(self, padding=""):
        string = "\n" + padding + "Content Size: {}"
        string = string.format(self.content_size)
        return super(ColumnFixedLengthSignature, self).stringify(padding) + string

    def update(self, serial_type, count=1, variable_length_serial_types=None):

        if serial_type != self.serial_type:
            log_message = "Specified serial type: {} does not match column fixed-length signature serial type: {} " \
                          "index: {} and name: {}"
            log_message = log_message.format(serial_type, self.serial_type, self.index, self.name)
            self._logger.error(log_message)
            raise ValueError(log_message)

        if variable_length_serial_types:
            log_message = "Variable length serial types: {} specified for column fixed-length signature " \
                          "index: {} and name: {}"
            log_message = log_message.format(variable_length_serial_types, self.index, self.name)
            self._logger.error(log_message)
            raise ValueError(log_message)

        self.count += count


class ColumnVariableLengthSignature(ColumnSignature):

    __metaclass__ = ABCMeta

    def __init__(self, index, name, serial_type, count=1):

        super(ColumnVariableLengthSignature, self).__init__(index, name, serial_type, count)

        """

        Note: The variable length serial types is a dictionary where:
              variable_length_serial_types[variable length serial type] = count of variable length serial type in column

        """

        self.variable_length_serial_types = None

    def stringify(self, padding=""):
        string = "\n" + padding + "Variable Length Serial Types: {}"
        string = string.format(self.variable_length_serial_types)
        return super(ColumnVariableLengthSignature, self).stringify(padding) + string

    def get_variable_length_serial_type_probability(self, variable_length_serial_type):

        """



        Note:  A value of None will be returned if the number of rows is not set.

        :param variable_length_serial_type:

        :return:

        """

        if self._number_of_rows:
            return float(self.variable_length_serial_types[variable_length_serial_type]) / self._number_of_rows
        return None


class ColumnReducedVariableLengthSignature(ColumnVariableLengthSignature):

    """

    ColumnReducedVariableLengthSignature



    Note:  This class is used where the serial types for variable length signatures are reduced and therefore
           are either -1 (for BLOB) or -2 (for TEXT).

    """

    def __init__(self, index, name, serial_type, count, variable_length_serial_types):

        if serial_type not in [-2, -1]:
            log_message = "Invalid serial type: {} for column reduced variable length signature index: {} and name: {}"
            log_message = log_message.format(serial_type, self.index, self.name)
            self._logger.error(log_message)
            raise ValueError(log_message)

        if not count:
            log_message = "Count not specified for column reduced variable length signature index: {} and name: {} " \
                          "for serial type: {} and variable length serial types: {}."
            log_message = log_message.format(index, name, serial_type, variable_length_serial_types)
            self._logger.error(log_message)
            raise ValueError(log_message)

        if not variable_length_serial_types:
            log_message = "Variable length serial types not specified for column reduced variable length signature " \
                          "index: {} and name: {} for serial type: {} and count: {}."
            log_message = log_message.format(index, name, serial_type, count)
            self._logger.error(log_message)
            raise ValueError(log_message)

        super(ColumnReducedVariableLengthSignature, self).__init__(index, name, serial_type, count)

        self.variable_length_serial_types = variable_length_serial_types

    def update(self, serial_type, count=None, variable_length_serial_types=None):

        if serial_type != self.serial_type:
            log_message = "Specified serial type: {} does not match column reduced variable length signature serial " \
                          "type: {} index: {} and name: {}"
            log_message = log_message.format(serial_type, self.serial_type, self.index, self.name)
            self._logger.error(log_message)
            raise ValueError(log_message)

        if not count:
            log_message = "Count not specified for column reduced variable length signature index: {} and name: {} " \
                          "for serial type: {} and variable length serial types: {}."
            log_message = log_message.format(self.index, self.name, serial_type, variable_length_serial_types)
            self._logger.error(log_message)
            raise ValueError(log_message)

        if not variable_length_serial_types:
            log_message = "Variable length serial types not specified for column reduced variable length signature " \
                          "index: {} and name: {} for serial type: {} and count: {}."
            log_message = log_message.format(self.index, self.name, serial_type, count)
            self._logger.error(log_message)
            raise ValueError(log_message)

        self.count += count

        for variable_length_serial_type, variable_length_serial_type_count in variable_length_serial_types.iteritems():
            if variable_length_serial_type in self.variable_length_serial_types:
                self.variable_length_serial_types[variable_length_serial_type] += variable_length_serial_type_count
            else:
                self.variable_length_serial_types[variable_length_serial_type] = variable_length_serial_type_count


class ColumnNonReducedVariableLengthSignature(ColumnVariableLengthSignature):

    """

    ColumnNonReducedVariableLengthSignature



    Note:  This class is used where the serial types for variable length signatures are not reduced and therefore
           are greater or equal to 12.

    """

    def __init__(self, index, name, serial_type):

        if serial_type < 12:
            log_message = "Invalid serial type: {} for column non-reduced variable length signature index: {} " \
                          "and name: {}"
            log_message = log_message.format(serial_type, self.index, self.name)
            self._logger.error(log_message)
            raise ValueError(log_message)

        super(ColumnNonReducedVariableLengthSignature, self).__init__(index, name, serial_type)

        self.variable_length_serial_types = {}

        # A BLOB that is (N-12)/2 bytes in length
        if self.serial_type >= 12 and self.serial_type % 2 == 0:
            self.variable_length_serial_types[self.serial_type] = 1
            self.serial_type = -1

        # A string in the database encoding and is (N-13)/2 bytes in length  (The nul terminator is omitted)
        elif self.serial_type >= 13 and self.serial_type % 2 == 1:
            self.variable_length_serial_types[self.serial_type] = 1
            self.serial_type = -2

        else:
            log_message = "Invalid serial type: {} for column non-reduced variable length signature index: {} and " \
                          "name: {}"
            log_message = log_message.format(serial_type, self.index, self.name)
            self._logger.error(log_message)
            raise ValueError(log_message)

    def update(self, serial_type, count=None, variable_length_serial_types=None):

        if serial_type < 12:
            log_message = "Invalid serial type: {} for column non-reduced variable length signature index: {} " \
                          "and name: {}"
            log_message = log_message.format(serial_type, self.index, self.name)
            self._logger.error(log_message)
            raise ValueError(log_message)

        if count:
            log_message = "Count specified for column non-reduced variable length signature index: {} and name: {} " \
                          "for serial type: {} and variable length serial types: {}."
            log_message = log_message.format(self.index, self.name, serial_type, variable_length_serial_types)
            self._logger.error(log_message)
            raise ValueError(log_message)

        if variable_length_serial_types:
            log_message = "Variable length serial types specified for column non-reduced variable length signature " \
                          "index: {} and name: {} for serial type: {} and count: {}."
            log_message = log_message.format(self.index, self.name, serial_type, count)
            self._logger.error(log_message)
            raise ValueError(log_message)

        self.count += 1

        # A BLOB that is (N-12)/2 bytes in length
        if serial_type >= 12 and serial_type % 2 == 0:

            if self.serial_type != -1:
                log_message = "Specified serial type: {} does not equate to column non-reduced variable length " \
                              "signature serial type: {} index: {} and name: {}"
                log_message = log_message.format(serial_type, self.serial_type, self.index, self.name)
                self._logger.error(log_message)
                raise ValueError(log_message)

        # A string in the database encoding and is (N-13)/2 bytes in length  (The nul terminator is omitted)
        elif serial_type >= 13 and serial_type % 2 == 1:

            if self.serial_type != -2:
                log_message = "Specified serial type: {} does not equate to column non-reduced variable length " \
                              "signature serial type: {} index: {} and name: {}"
                log_message = log_message.format(serial_type, self.serial_type, self.index, self.name)
                self._logger.error(log_message)
                raise ValueError(log_message)

        else:

            log_message = "Invalid serial type: {} for column non-reduced variable length signature index: {} and " \
                          "name: {}"
            log_message = log_message.format(serial_type, self.index, self.name)
            self._logger.error(log_message)
            raise ValueError(log_message)

        if serial_type in self.variable_length_serial_types:
            self.variable_length_serial_types[serial_type] += 1
        else:
            self.variable_length_serial_types[serial_type] = 1
