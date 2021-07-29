from abc import ABCMeta
from binascii import hexlify
from logging import getLogger
from re import sub
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.exception import RecordParsingError
from sqlite_dissect.utilities import decode_varint
from sqlite_dissect.utilities import get_md5_hash
from sqlite_dissect.utilities import get_record_content
from sqlite_dissect.utilities import get_serial_type_signature

"""

payload.py

This script holds the objects used for parsing payloads from the cells in SQLite b-tree pages for
index leaf, index  interior, and table leaf.  (Table Interior pages do not have payloads in their cells.)

This script holds the following object(s):
Payload(object)
Record(Payload)
RecordColumn(object)

"""


class Payload(object):

    __metaclass__ = ABCMeta

    def __init__(self):

        self.start_offset = None
        self.byte_size = None
        self.end_offset = None

        self.has_overflow = False
        self.bytes_on_first_page = None
        self.overflow_byte_size = None

        self.header_byte_size = None
        self.header_byte_size_varint_length = None
        self.header_start_offset = None
        self.header_end_offset = None
        self.body_start_offset = None
        self.body_end_offset = None

        self.md5_hex_digest = None

        self.record_columns = []
        self.serial_type_signature = ""

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding="", print_record_columns=True):
        string = padding + "Start Offset: {}\n" \
                 + padding + "End Offset: {}\n" \
                 + padding + "Byte Size: {}\n" \
                 + padding + "MD5 Hex Digest: {}\n" \
                 + padding + "Header Byte Size: {}\n" \
                 + padding + "Header Byte Size VARINT Length: {}\n" \
                 + padding + "Header Start Offset: {}\n" \
                 + padding + "Header End Offset: {}\n" \
                 + padding + "Body Start Offset: {}\n" \
                 + padding + "Body End Offset: {}\n" \
                 + padding + "Has Overflow: {}\n" \
                 + padding + "Bytes on First Page: {}\n" \
                 + padding + "Overflow Byte Size: {}\n" \
                 + padding + "Serial Type Signature: {}"
        string = string.format(self.start_offset,
                               self.end_offset,
                               self.byte_size,
                               self.md5_hex_digest,
                               self.header_byte_size,
                               self.header_byte_size_varint_length,
                               self.header_start_offset,
                               self.header_end_offset,
                               self.body_start_offset,
                               self.body_end_offset,
                               self.has_overflow,
                               self.bytes_on_first_page,
                               self.overflow_byte_size,
                               self.serial_type_signature)
        if print_record_columns:
            for record_column in self.record_columns:
                string += "\n" + padding + "Record Column:\n{}".format(record_column.stringify(padding + "\t"))
        return string


class Record(Payload):

    def __init__(self, page, payload_offset, payload_byte_size, bytes_on_first_page=None, overflow=bytearray()):

        super(Record, self).__init__()

        logger = getLogger(LOGGER_NAME)

        if bytes_on_first_page is None:

            bytes_on_first_page = payload_byte_size

            if overflow:
                log_message = "Bytes on first page not specified on page in record when overflow was (hex): {}."
                log_message = log_message.format(hexlify(overflow))
                logger.error(log_message)
                raise RecordParsingError(log_message)

        if bytes_on_first_page < payload_byte_size and not overflow:
            log_message = "Bytes on first page: {} less than payload byte size: {} on page with overflow not set."
            log_message = log_message.format(bytes_on_first_page, payload_byte_size)
            logger.error(log_message)
            raise RecordParsingError(log_message)

        if bytes_on_first_page > payload_byte_size:
            log_message = "Bytes on first page: {} greater than payload byte size: {} on page."
            log_message = log_message.format(bytes_on_first_page, payload_byte_size)
            logger.error(log_message)
            raise RecordParsingError(log_message)

        self.start_offset = payload_offset
        self.byte_size = payload_byte_size
        self.end_offset = self.start_offset + bytes_on_first_page

        self.has_overflow = False if not overflow else True
        self.bytes_on_first_page = bytes_on_first_page
        self.overflow_byte_size = self.byte_size - self.bytes_on_first_page

        if self.overflow_byte_size == 0 and overflow:
            log_message = "Overflow determined to exist with byte size: {} on page with overflow set: {}."
            log_message = log_message.format(self.overflow_byte_size, hexlify(overflow))
            logger.error(log_message)
            raise RecordParsingError(log_message)

        self.header_byte_size, self.header_byte_size_varint_length = decode_varint(page, self.start_offset)
        self.header_start_offset = self.start_offset
        self.header_end_offset = self.start_offset + self.header_byte_size
        self.body_start_offset = self.header_end_offset
        self.body_end_offset = self.end_offset

        current_page_record_content = page[self.start_offset:self.end_offset]

        total_record_content = current_page_record_content + overflow

        if len(total_record_content) != self.byte_size:
            log_message = "The record content was found to be a different length of: {} than the specified byte " \
                          "size: {} on page."
            log_message = log_message.format(len(total_record_content), self.byte_size)
            logger.error(log_message)
            raise RecordParsingError(log_message)

        self.md5_hex_digest = get_md5_hash(total_record_content)

        current_header_offset = self.header_byte_size_varint_length
        current_body_offset = 0
        column_index = 0
        while current_header_offset < self.header_byte_size:

            serial_type, serial_type_varint_length = decode_varint(total_record_content, current_header_offset)

            self.serial_type_signature += str(get_serial_type_signature(serial_type))

            record_column_md5_hash_string = total_record_content[current_header_offset:
                                                                 current_header_offset + serial_type_varint_length]

            body_content = total_record_content[self.header_byte_size:self.byte_size]

            content_size, value = get_record_content(serial_type, body_content, current_body_offset)

            """

            Note: If content_size == 0 then this will read out no data

            """

            record_column_md5_hash_string += body_content[current_body_offset:current_body_offset + content_size]

            record_column_md5_hex_digest = get_md5_hash(record_column_md5_hash_string)

            record_column = RecordColumn(column_index, serial_type, serial_type_varint_length,
                                         content_size, value, record_column_md5_hex_digest)

            self.record_columns.append(record_column)

            current_header_offset += serial_type_varint_length
            current_body_offset += content_size
            column_index += 1


class RecordColumn(object):

    def __init__(self, index, serial_type, serial_type_varint_length, content_size, value, md5_hex_digest):
        self.index = index
        self.serial_type = serial_type
        self.serial_type_varint_length = serial_type_varint_length
        self.content_size = content_size
        self.value = value
        self.md5_hex_digest = md5_hex_digest

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding=""):
        string = padding + "Index: {}\n" \
                 + padding + "Serial Type: {}\n" \
                 + padding + "Serial Type VARINT Length: {}\n" \
                 + padding + "Content Size: {}\n" \
                 + padding + "Value: {}\n" \
                 + padding + "MD5 Hex Digest: {}"
        return string.format(self.index,
                             self.serial_type,
                             self.serial_type_varint_length,
                             self.content_size,
                             self.value,
                             self.md5_hex_digest)
