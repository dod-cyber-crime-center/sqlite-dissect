from struct import unpack
from warnings import warn
from sqlite_dissect.carving.utilities import calculate_body_content_size
from sqlite_dissect.carving.utilities import calculate_serial_type_definition_content_length_min_max
from sqlite_dissect.carving.utilities import decode_varint_in_reverse
from sqlite_dissect.carving.utilities import get_content_size
from sqlite_dissect.constants import BLOB_SIGNATURE_IDENTIFIER
from sqlite_dissect.constants import CELL_LOCATION
from sqlite_dissect.constants import FILE_TYPE
from sqlite_dissect.constants import TEXT_SIGNATURE_IDENTIFIER
from sqlite_dissect.exception import CellCarvingError
from sqlite_dissect.exception import InvalidVarIntError
from sqlite_dissect.file.database.page import BTreeCell
from sqlite_dissect.file.database.payload import Payload
from sqlite_dissect.file.database.payload import RecordColumn
from sqlite_dissect.utilities import decode_varint
from sqlite_dissect.utilities import encode_varint
from sqlite_dissect.utilities import get_md5_hash
from sqlite_dissect.utilities import get_record_content
from sqlite_dissect.utilities import get_serial_type_signature

"""

carved_cell.py

This script holds the objects used for carving cells from the unallocated and freeblock space in SQLite
b-tree pages used in conjunction with other classes in the carving package.  These objects subclass their
respective higher level SQLite database object type and add to them while parsing the data in a different way.

This script holds the following object(s):
CarvedBTreeCell(BTreeCell)
CarvedRecord(Payload)
CarvedRecordColumn(RecordColumn)

"""


class CarvedBTreeCell(BTreeCell):

    """

    This class will be responsible for carving a b-tree cell to the best it can out of a block of data either from
    unallocated data or freeblocks.  Since the header to freeblocks can be overwritten meaning at most the first
    serial type identifier could be overwritten in the record, a list of first column serial types can be specified.
    The header of the record is in the following form:
    [ HEADER [ HEADER_BYTE_SIZE SERIAL_TYPE_1 ... SERIAL_TYPE_N] ][ BODY [ BODY_CONTENT_1 ... BODY_CONTENT_N ] ]
    For table leaf cells which are mainly being focused on here the cell is in the following format.

    Since unallocated space can contain freeblocks, this class will be used for both use cases of carving from
    unallocated space and freeblocks.

    If the carved b-tree cell has first column serial types set, a probabilistic flag will be set in both the carved
    b-tree cell, record, and record column indicating that not all fields were completely deterministic.

    Table interior, index interior, index leaf pages, and additional use cases still need to be accounted for.


    """

    def __init__(self, version, file_offset, source, page_number, location, index, data,
                 serial_type_definition_start_offset, serial_type_definition_end_offset, cutoff_offset,
                 number_of_columns, signature, first_column_serial_types=None, freeblock_size=None):

        """



        Note:  The md5 hex digest is set to the md5 hash of the data between the start offset and end offset determined
               after the carving of the payload.  It is important to note that these offsets may not be correct and
               therefore the md5 hex digest is a best guess at what it may be.

        :param version:
        :param file_offset:
        :param source:
        :param page_number:
        :param location:
        :param index:
        :param data:
        :param serial_type_definition_start_offset:
        :param serial_type_definition_end_offset:
        :param cutoff_offset:
        :param number_of_columns:
        :param signature:
        :param first_column_serial_types:
        :param freeblock_size:

        :return:

        """

        """

        Below we initialize the super constructor by sending in the version number of the version sent in to be the
        page version number.  The location will specify where the cell was carved from, either freeblocks in b-tree
        cells or unallocated space in b-tree pages or any other pages.  The index will be 0..N for freeblock carvings
        or just 0 for unallocated space.  The offset for the serial type definition start will be sent in as the offset,
        however this will be updated as needed when carving processes are run against the preceding data, if applicable,
        to determine payload length, row id, payload header size, and the first serial type in the payload header
        depending on the size of the varint between those fields and which fields are needed depending on the cells
        being parsed:
        1.) Table Leaf B-Tree Cell:     PAYLOAD_LENGTH_VARINT ROW_ID_VARINT PAYLOAD [OVERFLOW_PAGE_NUMBER]
        2.) Table Interior B-Tree Cell: LEFT_CHILD_POINTER INTEGER_KEY_VARINT (the integer key is a row id) (no payload)
        3.) Index Leaf B-Tree Cell:     PAYLOAD_LENGTH_VARINT PAYLOAD [OVERFLOW_PAGE_NUMBER]
        4.) Index Interior B-Tree Cell: LEFT_CHILD_POINTER PAYLOAD_LENGTH_VARINT PAYLOAD [OVERFLOW_PAGE_NUMBER]

        Better support needs to be done for supporting other cell types than the table leaf cell which is focused on
        here.

        """

        super(CarvedBTreeCell, self).__init__(version, version.version_number, file_offset, page_number,
                                              index, serial_type_definition_start_offset, source, location)

        """
        
        Since versioning is not implemented for rollback journal files we are going to set the version number to -1
        here.  This is done since rollback journals store previous data to what is in the SQLite database file as
        opposed to the WAL file where the most recent data in the WAL file reflects the most current state.
        
        """

        if source is FILE_TYPE.ROLLBACK_JOURNAL:
            self.version_number = -1
            self.page_version_number = -1

        self.payload = CarvedRecord(location, data, serial_type_definition_start_offset,
                                    serial_type_definition_end_offset, cutoff_offset, number_of_columns, signature,
                                    first_column_serial_types, freeblock_size, version.page_size)

        """

        After calling the above super constructor and setting the payload, we are left with a few more fields that
        need to be accounted for in the BTreeCell class.  These fields are as follows:
        1.) self.start_offset:      This is originally set to the serial_type_definition_start_offset through the super
                                    constructor but needs to be updated based on what is determined after carving the
                                    payload.
        2.) self.end_offset:        Updated after carving of the payload.
        3.) self.byte_size:         Calculated from the start and end offset after carving of the payload.
        4.) self.md5_hex_digest:    This is set to the md5 hash of the data between the start offset and end offset
                                    determined after the carving of the payload.  It is important to note that these
                                    offsets may not be correct and therefore the md5 hex digest is a best guess at what
                                    it may be.

        """

        self.start_offset = self.payload.cell_start_offset
        self.end_offset = self.payload.cell_end_offset

        self.byte_size = self.end_offset - self.start_offset
        self.md5_hex_digest = get_md5_hash(data[self.start_offset:self.end_offset])

        """

        Additionally to the fields in the BTreeCell class, we add truncated fields to signify if the record was
        truncated at either the beginning or ending.

        """

        self.truncated_beginning = self.payload.truncated_beginning
        self.truncated_ending = self.payload.truncated_ending

        self.row_id = "Unknown"

    def stringify(self, padding=""):
        string = "\n"\
                 + padding + "Truncated Beginning: {}\n" \
                 + padding + "Truncated Ending: {}"
        string = string.format(self.truncated_beginning,
                               self.truncated_ending)
        return super(CarvedBTreeCell, self).stringify(padding) + string


class CarvedRecord(Payload):

    def __init__(self, location, data, serial_type_definition_start_offset, serial_type_definition_end_offset,
                 cutoff_offset, number_of_columns, signature, first_column_serial_types=None,
                 freeblock_size=None, page_size=None):

        super(CarvedRecord, self).__init__()

        """

        Note:  The overflow fields below will stay their default values of False and None initialized in the super
               class:

               self.has_overflow = False
               self.bytes_on_first_page = None
               self.overflow_byte_size = None

               There is a TODO in reference to figuring out the best way to handle overflow.  Keep in mind that a lot
               of times the end portion of a cell may be overwritten, especially in a freeblock, since SQLite adds cells
               from the ending of the unallocated or freeblock content which would in turn overwrite the four byte
               overflow page number.  However, it is possible to calculate if the entry had overflow if the payload
               size is correctly determined.

        """

        self.start_offset = None
        self.byte_size = None
        self.end_offset = None

        self.header_byte_size = None
        self.header_byte_size_varint_length = None
        self.header_start_offset = None
        self.header_end_offset = None
        self.body_start_offset = None
        self.body_end_offset = None

        self.md5_hex_digest = None

        self.location = location
        self.serial_type_definition_start_offset = serial_type_definition_start_offset
        self.serial_type_definition_end_offset = serial_type_definition_end_offset
        self.number_of_columns = number_of_columns
        self.first_column_serial_types = first_column_serial_types
        self.freeblock_size = freeblock_size
        self.serial_type_definition_size = \
            self.serial_type_definition_end_offset - self.serial_type_definition_start_offset

        self.cutoff_offset = cutoff_offset
        self.truncated_beginning = False
        self.truncated_ending = False

        record_column_md5_hash_strings = [""] * self.number_of_columns

        column_index = 0
        body_byte_size = 0

        serial_type_definition_content_size = calculate_body_content_size(
            data[self.serial_type_definition_start_offset:self.serial_type_definition_end_offset])

        if self.serial_type_definition_start_offset == 0:

            if self.location == CELL_LOCATION.UNALLOCATED_SPACE:
                warn("unsupported", RuntimeWarning)

                """
                
                We do not know what the header amount could have been here.  We could check in reference to the
                header + byte array ( == 10 for table leaf cell) but we do not seem to gain a lot from this.
                
                We could also use probability on row and columns to figure out what the first column type is here
                (using a row signatures) or apply probability on the record and record column.
                
                """

            elif self.location == CELL_LOCATION.FREEBLOCK:

                # All 4 fields are 1 byte
                header_byte_size_varint_length = 1
                header_byte_size = header_byte_size_varint_length + self.serial_type_definition_size + 1
                payload_byte_size = self.freeblock_size - 2
                body_content_size = payload_byte_size - header_byte_size

                first_serial_type_varint_length = 1
                first_serial_type_content_size = body_content_size - serial_type_definition_content_size

                if first_serial_type_content_size > int('1111111', 2):
                    warn("first serial type too big", RuntimeWarning)

                matching_serial_types = []
                for serial_type in self.first_column_serial_types:
                    if get_content_size(serial_type) == first_serial_type_content_size or serial_type in \
                            [BLOB_SIGNATURE_IDENTIFIER, TEXT_SIGNATURE_IDENTIFIER]:
                        matching_serial_types.append(serial_type)

                if len(matching_serial_types) > 1:
                    warn("multiple matching, need to use probability")

                elif len(matching_serial_types) == 1:

                    first_serial_type = matching_serial_types[0]

                    self.serial_type_signature += str(get_serial_type_signature(first_serial_type))

                    record_column_md5_hash_strings[column_index] = ""

                    self.serial_type_definition_size += first_serial_type_varint_length

                    first_carved_record_column = CarvedRecordColumn(column_index, first_serial_type,
                                                                    first_serial_type_varint_length,
                                                                    first_serial_type_content_size)
                    first_carved_record_column.truncated_first_serial_type = True
                    self.truncated_beginning = True
                    self.record_columns.append(first_carved_record_column)
                    column_index += 1
                    body_byte_size += first_serial_type_content_size

                else:
                    warn("could not find matching serial types", RuntimeWarning)

            else:
                raise CellCarvingError()

        elif self.serial_type_definition_start_offset == 1:

            if self.location == CELL_LOCATION.UNALLOCATED_SPACE:
                warn("unsupported", RuntimeWarning)

                """
                
                A way to address this may be checking if the signature does not have a -1 or -2 (blob or string), then 
                check the single byte to get the serial type and then check this against the signatures.  If it does 
                not, then use the probability but we will not know hte length of the type unless the cutoff is
                correctly implemented.  Freeblocks do not count since the size may not match (since they need two bytes)
                but you may be able to check on one byte.
                
                """

            elif self.location == CELL_LOCATION.FREEBLOCK:

                """
                
                The row id was 2 varint length in bytes 128 <= x <= 16383 or payload >= 2 varint bytes (or both) 
                or header size.  Use cases for this need to be investigated further.
                
                """

                first_serial_type, first_serial_type_varint_length = \
                    decode_varint(data, self.serial_type_definition_start_offset - 1)

                if first_serial_type_varint_length != 1:
                    raise CellCarvingError()

                if get_serial_type_signature(first_serial_type) in self.first_column_serial_types:

                    self.serial_type_definition_size += first_serial_type_varint_length

                    first_serial_type_content_size = get_content_size(first_serial_type)

                    header_byte_size_varint_length = 1

                    if self.serial_type_definition_size >= int('1111111' * 1, 2):
                        header_byte_size_varint_length += 1
                    elif self.serial_type_definition_size >= int('1111111' * 2, 2):
                        header_byte_size_varint_length += 2

                    header_byte_size = self.serial_type_definition_size + header_byte_size_varint_length

                    body_content_size = serial_type_definition_content_size + first_serial_type_content_size

                    payload_byte_size = header_byte_size + body_content_size

                    self.serial_type_signature += str(get_serial_type_signature(first_serial_type))

                    record_column_md5_hash_strings[column_index] = data[self.serial_type_definition_start_offset - 1:
                                                                        self.serial_type_definition_start_offset]

                    first_carved_record_column = CarvedRecordColumn(column_index, first_serial_type,
                                                                    first_serial_type_varint_length,
                                                                    first_serial_type_content_size)

                    self.record_columns.append(first_carved_record_column)
                    column_index += 1
                    body_byte_size += first_serial_type_content_size

                else:
                    warn("unable to find serial type with 1 preceding", RuntimeWarning)

            else:
                raise CellCarvingError()

        elif self.serial_type_definition_start_offset >= 2:

            if self.location == CELL_LOCATION.UNALLOCATED_SPACE:
                warn("unsupported unallocated space with serial type definition start offset >= 2", RuntimeWarning)

            elif self.location == CELL_LOCATION.FREEBLOCK:

                """
                
                There are three use cases that can occur here:
                1.) Everything was overwritten up to this point and there is nothing more to carve
                2.) Freeblock cutting off beginning with size up to the first serial type
                3.) Freeblock cutting off beginning but not the first serial type and the header size/row id may still
                    be in tact somewhat (payload must be overwritten partially in best case)
                
                """

                # First check first byte against serial types but also parse freeblock size and check which is best
                freeblock_size = unpack(b">H", data[self.serial_type_definition_start_offset - 2:
                                                    self.serial_type_definition_start_offset])[0]
                freeblock_first_serial_type_min, freeblock_first_serial_type_max = \
                    calculate_serial_type_definition_content_length_min_max(None, 1)

                header_byte_size_varint_length = 1
                header_byte_size = header_byte_size_varint_length + self.serial_type_definition_size + 1

                body_content_size_min = serial_type_definition_content_size + freeblock_first_serial_type_min
                body_content_size_max = serial_type_definition_content_size + freeblock_first_serial_type_max

                payload_size_min = header_byte_size + body_content_size_min
                payload_size_max = header_byte_size + body_content_size_max

                freeblock_size_valid = False
                if freeblock_size >= payload_size_min and freeblock_size <= payload_size_max:
                    freeblock_size_valid = True

                next_free_block_offset = None
                if freeblock_size_valid and self.serial_type_definition_start_offset >= 4:
                    next_free_block_offset = unpack(b">H", data[self.serial_type_definition_start_offset - 4:
                                                                self.serial_type_definition_start_offset - 2])[0]
                    if next_free_block_offset >= page_size:
                        freeblock_size_valid = False

                """
                
                Check first serial types not in freeblock size first byte.
                
                """

                # Check freeblock size valid over first serial type
                if freeblock_size_valid:

                    # All 4 fields are 1 byte
                    header_byte_size_varint_length = 1
                    header_byte_size = header_byte_size_varint_length + self.serial_type_definition_size + 1
                    payload_byte_size = freeblock_size - 2
                    body_content_size = payload_byte_size - header_byte_size

                    first_serial_type_varint_length = 1
                    first_serial_type_content_size = body_content_size - serial_type_definition_content_size

                    if first_serial_type_content_size > int('1111111', 2):
                        warn("first serial type too big", RuntimeWarning)

                    matching_serial_types = []
                    for serial_type in self.first_column_serial_types:
                        if get_content_size(serial_type) == first_serial_type_content_size or serial_type in \
                                [BLOB_SIGNATURE_IDENTIFIER, TEXT_SIGNATURE_IDENTIFIER]:
                            matching_serial_types.append(serial_type)

                    if len(matching_serial_types) > 1:
                        warn("multiple matching, need to use probability")

                    elif len(matching_serial_types) == 1:

                        first_serial_type = matching_serial_types[0]

                        self.serial_type_signature += str(get_serial_type_signature(first_serial_type))

                        record_column_md5_hash_strings[column_index] = ""

                        self.serial_type_definition_size += first_serial_type_varint_length

                        first_carved_record_column = CarvedRecordColumn(column_index, first_serial_type,
                                                                        first_serial_type_varint_length,
                                                                        first_serial_type_content_size)
                        first_carved_record_column.truncated_first_serial_type = True
                        self.truncated_beginning = True
                        self.record_columns.append(first_carved_record_column)
                        column_index += 1
                        body_byte_size += first_serial_type_content_size

                    else:
                        warn("could not find matching serial types", RuntimeWarning)

                else:

                    """
                    
                    There are two main use cases here:
                    1.) single byte varint 00-09
                    2.) multi byte varint (if in signature)
                    
                    A possible third use case may be a inner freeblock.
                    
                    """

                    simplified_variable_length_serial_types = [BLOB_SIGNATURE_IDENTIFIER, TEXT_SIGNATURE_IDENTIFIER]
                    text_or_blob_serial_type = \
                        any(i in first_column_serial_types for i in simplified_variable_length_serial_types)

                    if not text_or_blob_serial_type:

                        freeblock_size = None

                        # Check the previous two bytes if they exist:
                        if self.serial_type_definition_start_offset >= 3:
                            freeblock_size = unpack(b">H", data[self.serial_type_definition_start_offset - 3:
                                                                self.serial_type_definition_start_offset - 1])[0]

                        """

                        The row id was 2 varint length in bytes 128 <= x <= 16383 or payload >= 2 varint bytes (or both) 
                        or header size.  Use cases for this need to be investigated further.

                        """

                        first_serial_type, first_serial_type_varint_length = \
                            decode_varint(data, self.serial_type_definition_start_offset - 1)

                        if first_serial_type_varint_length != 1:

                            """
                            
                            Note:  Issues can occur here where the pattern matches something not in a serial type 
                            header that is a serial type.  For instance: 000000900302 will match a simple signature 
                            (freeblock) of [[02], [03]] which will result in [03] will match the 03 and detect 90 
                            as the first serial type where it could be the size of the freeblock in the form of 0090.
                            
                            """

                            raise CellCarvingError("Invalid first serial type varint size determined.  "
                                                   "Unable to carve due to probable false positive.")

                        if get_serial_type_signature(first_serial_type) in self.first_column_serial_types:

                            self.serial_type_definition_size += first_serial_type_varint_length

                            first_serial_type_content_size = get_content_size(first_serial_type)

                            header_byte_size_varint_length = 1

                            if self.serial_type_definition_size >= int('1111111' * 1, 2):
                                header_byte_size_varint_length += 1
                            elif self.serial_type_definition_size >= int('1111111' * 2, 2):
                                header_byte_size_varint_length += 2

                            header_byte_size = self.serial_type_definition_size + header_byte_size_varint_length

                            body_content_size = serial_type_definition_content_size + first_serial_type_content_size

                            payload_byte_size = header_byte_size + body_content_size

                            # Add one since row id, payload, or serial type header (not) >= 1 varint
                            calculated_freeblock_size = payload_byte_size + 2 + 1
                            freeblock_size_valid = False
                            if freeblock_size == calculated_freeblock_size:
                                freeblock_size_valid = True

                            next_free_block_offset = None
                            if freeblock_size_valid and self.serial_type_definition_start_offset >= 5:
                                next_free_block_offset = unpack(b">H",
                                                                data[self.serial_type_definition_start_offset - 5:
                                                                     self.serial_type_definition_start_offset - 3])[0]
                                if next_free_block_offset >= page_size:
                                    freeblock_size_valid = False

                            self.serial_type_signature += str(get_serial_type_signature(first_serial_type))

                            record_column_md5_hash_strings[column_index] = \
                                data[self.serial_type_definition_start_offset - 1:
                                     self.serial_type_definition_start_offset]

                            first_carved_record_column = CarvedRecordColumn(column_index, first_serial_type,
                                                                            first_serial_type_varint_length,
                                                                            first_serial_type_content_size)
                            self.record_columns.append(first_carved_record_column)

                            column_index += 1
                            body_byte_size += first_serial_type_content_size

                        else:
                            warn("unable to find serial type with 1 preceding", RuntimeWarning)

                    else:

                        first_serial_type = None
                        first_serial_type_varint_length = None
                        try:

                            first_serial_type, first_serial_type_varint_length = \
                                decode_varint_in_reverse(data, self.serial_type_definition_start_offset, 5)

                        except InvalidVarIntError:
                            pass

        if self.first_column_serial_types and not len(self.record_columns):

            first_serial_type = first_column_serial_types[0]
            if signature.total_records == 0:
                # Set as null for now
                first_serial_type = 0
            if len(first_column_serial_types) != 1:
                simplified_probabilistic_signature = signature.simplified_probabilistic_signature
                if simplified_probabilistic_signature:
                    # Found probability otherwise it is a schema without probability
                    first_probabilistic_column_serial_types = simplified_probabilistic_signature[0]
                    first_serial_type = max(first_probabilistic_column_serial_types,
                                            key=lambda first_probabilistic_column_serial_type:
                                            first_probabilistic_column_serial_type[1])[0]
            first_serial_type_varint_length = 1
            self.serial_type_signature += str(get_serial_type_signature(first_serial_type))
            self.serial_type_definition_size += first_serial_type_varint_length
            if first_serial_type == TEXT_SIGNATURE_IDENTIFIER:
                first_serial_type = 12
            if first_serial_type == BLOB_SIGNATURE_IDENTIFIER:
                first_serial_type = 13
            first_serial_type_content_size = get_content_size(first_serial_type)
            first_carved_record_column = CarvedRecordColumn(column_index, first_serial_type,
                                                            first_serial_type_varint_length,
                                                            first_serial_type_content_size)
            first_carved_record_column.probabilistic_first_serial_type = True
            first_carved_record_column.truncated_first_serial_type = True
            self.truncated_beginning = True
            self.record_columns.append(first_carved_record_column)
            column_index += 1
            body_byte_size += first_serial_type_content_size

        """

        We iterate through the header and generate all of the carved record columns off of the header.  We know we have
        at least enough information in the header to be able to determine the types and size of the body regardless of
        if we have the body or not.  This is due to the expression being sent in determined from regular expressions
        which match the header, with the possible exception of the first serial type which if existing, has already
        been handled above.

        """

        current_header_offset = self.serial_type_definition_start_offset
        while current_header_offset < self.serial_type_definition_end_offset:

            serial_type, serial_type_varint_length = decode_varint(data, current_header_offset)

            serial_type_varint_end_offset = current_header_offset + serial_type_varint_length

            if serial_type_varint_end_offset > self.serial_type_definition_end_offset:
                raise CellCarvingError()

            self.serial_type_signature += str(get_serial_type_signature(serial_type))

            record_column_md5_hash_strings[column_index] = data[current_header_offset:serial_type_varint_end_offset]

            content_size = get_content_size(serial_type)

            carved_record_column = CarvedRecordColumn(column_index, serial_type, serial_type_varint_length,
                                                      content_size)
            self.record_columns.append(carved_record_column)

            current_header_offset += serial_type_varint_length
            body_byte_size += content_size
            column_index += 1

        if len(self.record_columns) != number_of_columns:
            raise CellCarvingError()

        self.body_start_offset = self.serial_type_definition_end_offset
        self.body_end_offset = self.serial_type_definition_end_offset + body_byte_size

        if self.body_end_offset > len(data):
            self.truncated_ending = True

        """
        
        Note:  This does not currently work for multiple options in the first or variable length serial types.
        
        """

        # First truncated column field
        current_body_offset = self.body_start_offset
        for carved_record_column in self.record_columns:

            if (current_body_offset + carved_record_column.content_size) > len(data):
                carved_record_column.truncated_value = True
                if current_body_offset < len(data):
                    carved_record_column.value = data[current_body_offset:]
                    record_column_md5_hash_strings[carved_record_column.index] += data[current_body_offset:]
                    carved_record_column.md5_hex_digest = \
                        get_md5_hash(record_column_md5_hash_strings[carved_record_column.index])

            else:

                """
                
                This means that: offset + content_size <= len(data)
                
                """

                value_data = data[current_body_offset:current_body_offset + carved_record_column.content_size]
                content_size, value = get_record_content(carved_record_column.serial_type, value_data)

                if content_size != carved_record_column.content_size:
                    raise CellCarvingError()
                carved_record_column.value = value
                record_column_md5_hash_strings[carved_record_column.index] += value_data
                carved_record_column.md5_hex_digest = \
                    get_md5_hash(record_column_md5_hash_strings[carved_record_column.index])

            current_body_offset += carved_record_column.content_size

        if self.body_end_offset != current_body_offset:
            raise CellCarvingError()

        # This assumes the length of the header is 1 byte (most cases it will or would mean # of rows > 127 for table).
        self.header_byte_size = self.serial_type_definition_size + 1

        self.header_byte_size_varint = encode_varint(self.header_byte_size)
        self.header_byte_size_varint_length = len(self.header_byte_size_varint)

        self.payload_byte_size = self.header_byte_size + body_byte_size

        self.payload_byte_size_varint = encode_varint(self.payload_byte_size)
        self.payload_byte_size_varint_length = len(self.payload_byte_size_varint)

        # Below is relative to the unallocated space.  The "-1" is to account for the row id.
        self.cell_start_offset = self.serial_type_definition_start_offset - self.record_columns[0].\
            serial_type_varint_length - self.header_byte_size_varint_length - 1 - self.payload_byte_size_varint_length
        self.cell_end_offset = self.body_end_offset


class CarvedRecordColumn(RecordColumn):

    def __init__(self, index, serial_type, serial_type_varint_length, content_size):

        """

        Constructor.

        This method constructs the carved record column by calling it's super constructor and then setting a few
        additional fields for itself in reference to carving traits.

        If this carved record column was truncated (ie. the rest of the record was overwritten at some point), then
        the truncated value flag will be set to True.  If this is the case, the value may or may not be set depending
        if this column was the actually column that got cut off.  Past the first column that gets truncated, all
        following carved record columns will not have the value set.

        Keep in mind that the column value may be "None" if it was a NULL value in the database.  However, this will
        only be truly NULL if the field is not truncated.  If the field is truncated, then if it has a value of "None"
        it is due to the fact that it was unable to be obtained.

        The md5 hex digest will be the md5 of the found portions of the record column whether that just be the serial
        type header, serial type header and value, or serial type header and truncated value.

        It is also important to keep in mind that parts of the record could be overwritten without being detected
        resulting in some weird values.

        Note:  For reference the RecordColumn super class has the following attributes:
               1.) index
               2.) serial_type
               3.) serial_type_varint_length
               4.) content_size
               5.) value
               6.) md5_hex_digest

        :param index:
        :param serial_type:
        :param serial_type_varint_length:
        :param content_size:

        :return:

        """

        """

        Call to the constructor of the super record column class but specify "None" for the value and
        md5 hex digest since they aren't known at this time.

        """

        super(CarvedRecordColumn, self).__init__(index, serial_type, serial_type_varint_length, content_size,
                                                 None, None)

        self.simplified_serial_type = self.serial_type
        if self.serial_type >= 12 and self.serial_type % 2 == 0:
            self.simplified_serial_type = -1
        elif self.serial_type >= 13 and self.serial_type % 2 == 1:
            self.simplified_serial_type = -2

        """

        Note:  The below values are set to defaults and expected to be updated by the calling class if intended for use.

        """

        self.value = None
        self.md5_hex_digest = None

        self.truncated_first_serial_type = False
        self.truncated_value = False
        self.probabilistic = False

    def stringify(self, padding=""):
        string = "\n" \
                 + padding + "Simplified Serial Type: {}\n" \
                 + padding + "Truncated First Serial Type: {}\n" \
                 + padding + "Truncated Value: {}\n" \
                 + padding + "Probabilistic: {}"
        string = string.format(self.simplified_serial_type,
                               self.truncated_first_serial_type,
                               self.truncated_value,
                               self.probabilistic)
        return super(CarvedRecordColumn, self).stringify(padding) + string

    """

    If we have a the first column serial types set, then the full serial type definition (referring to the
    payload header excepting the header size) was not determined previously.  However, since freeblocks
    overwrite the first four bytes, assuming there is a payload size, row id, and serial type header size
    followed by the serial types (ie. a b-tree table leaf cell), at most the first serial type can be
    overwritten, or the first varint byte of a varint serial type if it is more than 1 byte in length.
    Again, this only accounts for b-tree table leaf cells and there is a TODO in reference to supporting
    other cell types.

    There are two use cases to address for the first column serial types:
    1.) Preceding bytes detected.
    2.) No Preceding bytes detected (or invalid varint from #1).

    1.) Preceding bytes detected:

    If there are bytes preceding the serial type definition start offset in the data, then we may be able
    to parse backwards in order to determine the first serial type and payload header size assuming the best
    case scenario and a b-tree table leaf, index interior, or index leaf cell since b-tree table interiors do
    not have payloads associated with their cells.  We also have to assume that the preceding bytes were not
    overwritten in some manner.

    The way we will check the first column serial type will be to see what serial types are possible for it.
    Remember that the first column serial types is an array of the different serial types that can exist and
    will be a subset of: [-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9] where -2 and -1 are varint serial types
    representing the TEXT and BLOB storage classes respectfully.

    If a varint serial type exists that can be more than 1 byte (TEXT or BLOB), we will call the
    decode_varint_in_reverse function in order to retrieve it.  However, before we do this we will AND the byte
    with 0x80 to see if the most significant bit is set (ie. msb_set = varint_serial_type_byte & 0x80).
    If the most significant bit is set, then it is likely this is not the least significant byte of a serial
    type since the least significant byte should never have the most significant bit set. Since all serial
    types that are not multi-byte serial types will be within the range of 0x00 to 0x09, this will tell us a
    few things.  The first thing is if the first serial type is a single byte varint serial type
    meaning it could be any of the serial types including TEXT and BLOB with the size of 57 or less with
    regards to the first serial type least significant byte:

    1.) TEXT:   Min single byte size: 0x0D = (13 - 13)/2 = 0
                Max single byte size: 0x7F = (127 - 13)/2 = 57

                Note:  However, there may be additional, preceding bytes signifying a larger size.

                Note:  The TEXT is "odd" and can be determined by checking if the byte > 13 and
                       if the byte % 2 == 1.  Similarly, if it the byte > 13 then if byte & 0x01,
                       it is also TEXT.

    2.) BLOB:   Min single byte size: 0x0C = (12 - 12)/2 = 0
                Max single byte size: 0x7E = (126 - 12)/2 = 57

                Note:  However, there may be additional, preceding bytes signifying a larger size.

                Note:  The BLOB is "even" and can be determined by checking if the byte > 12 and
                       if the byte % 2 == 0.  Similarly, if it the byte > 12 then if NOT byte & 0x01,
                       it is also BLOB.

    3.) All other serial types are single byte varints where 0x00 <= serial_type <= 0x09.

    Note:  The bytes 0x0A and 0x0B are not used and are currently reserved for expansion.  This in combination
           of the above use cases and those where the most significant bit is set cover all use cases for
           relating the preceding byte (the least significant byte of the possible multi-byte varint) to their
           respective serial type.  However, we still may not have the correct length of the serial types in
           respect to the variable length multi-byte varints for TEXT and BLOB with a size greater than 57.
           This will be determined by looking at preceding bytes, if existing, and accuracy will be depending
           on how many bytes preceding this byte remain and if it has not been overwritten in any way.

    If either the 0x0A, 0x0B or msb_set (varint_serial_type_byte & 0x80), then we do not have a serial type
    and we resort to the same use cases as #2 below since we have determined an invalid varint.

    If we do have a serial type where the byte is between 0x0C and 0x7F, then we have to look at the preceding
    bytes, if existing to hopefully determine if it is a portion of a larger varint determining a larger size
    for that data type.  In order to get the correct size of the serial type we call the
    decode_varint_in_reverse function to parse backwards until we either hit the 9 byte maximum for varints or
    find a most significant byte where the most significant bit is not set.  However, there is a chance we will
    run out of data in the array going backwards.  In order to facilitate this, the decode_varint_in_reverse
    returns three fields in the form of a tuple:
    (unsigned_integer_value, varint_relative_offset, truncated)
    Keep in mind that even if it was not truncated and found all bytes for the varint, the varint still may be
    incorrect due to use cases where it was overwritten with bytes that may be mistaken for valid varint bytes.

    If the variable length serial type turns out to be truncated, then we set that flag in the carved record
    since we can not be certain if it is either partially carved or completely erroneous.  We leave this in
    order to be addressed as needed when parsing the first serial type data content from the body.

    However, the function can also throw an InvalidVarIntError in which case the varint will be assumed to be
    overwritten in some way and we will default to the process explained further below where we do not have
    preceding bytes.  This is also true if we find a invalid serial type on the first preceding byte.

    Note:  There is a chance of false positives being returned by this function and validation checks need to
           be investigated in order to make this value more deterministic.  A TODO has been placed at the top
           of this script in reference to this issue.

    Note:  Also, 9 byte varints are not currently handled.  There are TODOs in references to 9 byte varint
           parsing in both this script and their respective parsing function scripts.

    2.) No Preceding bytes detected (or invalid varint from #1).

    If there are no bytes preceding the serial type definition start offset, we will assume the field is the
    one with the most probability.

    """

    """

    1.) Preceding bytes detected:

    In order to check if we have preceding bytes and then parse backwards through them we first check if
    the serial type definition start offset is greater than one.  If this is true, we know we have at least
    one preceding byte that we can check to see the serial type of.

    Keep in mind that although this will give us a serial type, it may be a byte overwritten by something else
    and is not completely deterministic.

    """
