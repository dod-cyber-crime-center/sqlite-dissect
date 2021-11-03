from binascii import hexlify
from hashlib import md5
from logging import getLogger
from re import compile
from struct import pack
from struct import unpack
from sqlite_dissect.constants import ALL_ZEROS_REGEX
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import OVERFLOW_HEADER_LENGTH
from sqlite_dissect.constants import BLOB_SIGNATURE_IDENTIFIER
from sqlite_dissect.constants import TEXT_SIGNATURE_IDENTIFIER
from sqlite_dissect.exception import InvalidVarIntError

"""

utilities.py

This script holds general utility functions for reference by the sqlite carving library.

This script holds the following function(s):
calculate_expected_overflow(overflow_byte_size, page_size)
decode_varint(byte_array, offset)
encode_varint(value)
get_class_instance(class_name)
get_md5_hash(string)
get_record_content(serial_type, record_body, offset=0)
get_serial_type_signature(serial_type)
has_content(byte_array)

"""


class DotDict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def calculate_expected_overflow(overflow_byte_size, page_size):
    overflow_pages = 0
    last_overflow_page_content_size = overflow_byte_size

    if overflow_byte_size > 0:
        while overflow_byte_size > 0:
            overflow_pages += 1
            last_overflow_page_content_size = overflow_byte_size
            overflow_byte_size = overflow_byte_size - page_size + OVERFLOW_HEADER_LENGTH

    return overflow_pages, last_overflow_page_content_size


def decode_varint(byte_array, offset=0):
    unsigned_integer_value = 0
    varint_relative_offset = 0

    for x in xrange(1, 10):

        varint_byte = ord(byte_array[offset + varint_relative_offset:offset + varint_relative_offset + 1])
        varint_relative_offset += 1

        if x == 9:
            unsigned_integer_value <<= 1
            unsigned_integer_value |= varint_byte
        else:
            msb_set = varint_byte & 0x80
            varint_byte &= 0x7f
            unsigned_integer_value |= varint_byte
            if msb_set == 0:
                break
            else:
                unsigned_integer_value <<= 7

    signed_integer_value = unsigned_integer_value
    if signed_integer_value & 0x80000000 << 32:
        signed_integer_value -= 0x10000000000000000

    return signed_integer_value, varint_relative_offset


def encode_varint(value):
    max_allowed = 0x7fffffffffffffff
    min_allowed = (max_allowed + 1) - 0x10000000000000000
    if value > max_allowed or value < min_allowed:
        log_message = "The value: {} is not able to be cast into a 64 bit signed integer for encoding."
        log_message = log_message.format(value)
        getLogger(LOGGER_NAME).error(log_message)
        raise InvalidVarIntError(log_message)

    byte_array = bytearray()

    value += 1 << 64 if value < 0 else 0

    if value & 0xff000000 << 32:

        byte = value & 0xff
        byte_array.insert(0, pack("B", byte))
        value >>= 8

        for _ in xrange(8):
            byte_array.insert(0, pack("B", (value & 0x7f) | 0x80))
            value >>= 7

    else:

        while value:
            byte_array.insert(0, pack("B", (value & 0x7f) | 0x80))
            value >>= 7

            if len(byte_array) >= 9:
                log_message = "The value: {} produced a varint with a byte array of length: {} beyond the 9 bytes " \
                              "allowed for a varint."
                log_message = log_message.format(value, len(byte_array))
                getLogger(LOGGER_NAME).error(log_message)
                raise InvalidVarIntError(log_message)

        byte_array[-1] &= 0x7f

    return byte_array


def get_class_instance(class_name):
    if class_name.find(".") != -1:
        path_array = class_name.split(".")
        module = ".".join(path_array[:-1])
        instance = __import__(module)
        for section in path_array[1:]:
            instance = getattr(instance, section)
        return instance
    else:
        log_message = "Class name: {} did not specify needed modules in order to initialize correctly."
        log_message = log_message.format(log_message)
        getLogger(LOGGER_NAME).error(log_message)
        raise ValueError(log_message)


def get_md5_hash(string):
    md5_hash = md5()
    md5_hash.update(string)
    return md5_hash.hexdigest().upper()


def get_record_content(serial_type, record_body, offset=0):
    # NULL
    if serial_type == 0:
        content_size = 0
        value = None

    # 8-bit twos-complement integer
    elif serial_type == 1:
        content_size = 1
        value = unpack(b">b", record_body[offset:offset + content_size])[0]

    # Big-endian 16-bit twos-complement integer
    elif serial_type == 2:
        content_size = 2
        value = unpack(b">h", record_body[offset:offset + content_size])[0]

    # Big-endian 24-bit twos-complement integer
    elif serial_type == 3:
        content_size = 3
        value_byte_array = '\0' + record_body[offset:offset + content_size]
        value = unpack(b">I", value_byte_array)[0]
        if value & 0x800000:
            value -= 0x1000000

    # Big-endian 32-bit twos-complement integer
    elif serial_type == 4:
        content_size = 4
        value = unpack(b">i", record_body[offset:offset + content_size])[0]

    # Big-endian 48-bit twos-complement integer
    elif serial_type == 5:
        content_size = 6
        value_byte_array = '\0' + '\0' + record_body[offset:offset + content_size]
        value = unpack(b">Q", value_byte_array)[0]
        if value & 0x800000000000:
            value -= 0x1000000000000

    # Big-endian 64-bit twos-complement integer
    elif serial_type == 6:
        content_size = 8
        value = unpack(b">q", record_body[offset:offset + content_size])[0]

    # Big-endian IEEE 754-2008 64-bit floating point number
    elif serial_type == 7:
        content_size = 8
        value = unpack(b">d", record_body[offset:offset + content_size])[0]

    # Integer constant 0 (schema format == 4)
    elif serial_type == 8:
        content_size = 0
        value = 0

    # Integer constant 1 (schema format == 4)
    elif serial_type == 9:
        content_size = 0
        value = 1

    # These values are not used/reserved and should not be found in sqlite files
    elif serial_type == 10 or serial_type == 11:
        raise Exception()

    # A BLOB that is (N-12)/2 bytes in length
    elif serial_type >= 12 and serial_type % 2 == 0:
        content_size = (serial_type - 12) / 2
        value = record_body[offset:offset + content_size]

    # A string in the database encoding and is (N-13)/2 bytes in length.  The nul terminator is omitted
    elif serial_type >= 13 and serial_type % 2 == 1:
        content_size = (serial_type - 13) / 2
        value = record_body[offset:offset + content_size]

    else:
        log_message = "Invalid serial type: {} at offset: {} in record body: {}."
        log_message = log_message.format(serial_type, offset, hexlify(record_body))
        getLogger(LOGGER_NAME).error(log_message)
        raise ValueError(log_message)

    return content_size, value


def get_serial_type_signature(serial_type):
    if serial_type >= 12:
        if serial_type % 2 == 0:
            return BLOB_SIGNATURE_IDENTIFIER
        elif serial_type % 2 == 1:
            return TEXT_SIGNATURE_IDENTIFIER
    return serial_type


def has_content(byte_array):
    pattern = compile(ALL_ZEROS_REGEX)
    if pattern.match(hexlify(byte_array)):
        return False
    return True
