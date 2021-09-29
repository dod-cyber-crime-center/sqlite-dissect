import struct
import unittest

from sqlite_dissect.utilities import calculate_expected_overflow, get_serial_type_signature, has_content, \
    get_record_content
from sqlite_dissect.constants import BLOB_SIGNATURE_IDENTIFIER, TEXT_SIGNATURE_IDENTIFIER


class TestRootUtilities(unittest.TestCase):
    """
    This class only tests functions at ../utilities.py and not the utilities.py files within submodules
    """

    def test_calculate_expected_overflow(self):
        # Test when the overflow_byte_size is <= 0
        result = calculate_expected_overflow(0, 5)
        self.assertEqual(0, result[0])  # overflow_pages
        self.assertEqual(0, result[1])  # last_overflow_page_content_size

        # Test when the overflow_byte_size and page_size are both > 0 but it fits on a single page
        result = calculate_expected_overflow(5, 10)
        self.assertEqual(1, result[0])  # overflow_pages
        self.assertEqual(5, result[1])  # last_overflow_page_content_size

        # Test when the overflow_byte_size and page_size are both > 0 and it doesn't fit on a single page
        result = calculate_expected_overflow(15, 10)
        self.assertEqual(3, result[0])  # overflow_pages
        self.assertEqual(3, result[1])  # last_overflow_page_content_size

    def test_get_record_content(self):
        test_string_array = list('this is a string')
        test_string_array = 'this is a string'.encode('utf-8')
        test_byte_array = struct.pack('=BHBL', 1, 2, 3, 4)

        # Test when serial_type is 0
        result = get_record_content(0, test_byte_array, 0)
        self.assertEqual(0, result[0])
        self.assertEqual(None, result[1])

        # Test when serial_type is 1
        result = get_record_content(1, test_byte_array, 0)
        self.assertEqual(1, result[0])
        # self.assertEqual(None, result[1])

        # Test when serial_type is 2
        result = get_record_content(2, test_byte_array, 0)
        self.assertEqual(2, result[0])
        # self.assertEqual(None, result[1])

        # Test when serial_type is 3
        result = get_record_content(3, test_byte_array, 0)
        self.assertEqual(3, result[0])
        # self.assertEqual(None, result[1])

        # Test when serial_type is 4
        result = get_record_content(4, test_byte_array, 0)
        self.assertEqual(4, result[0])
        # self.assertEqual(None, result[1])

        # Test when serial_type is 5
        result = get_record_content(5, test_byte_array, 0)
        self.assertEqual(6, result[0])
        # self.assertEqual(None, result[1])

        # Test when serial_type is 6
        result = get_record_content(6, test_byte_array, 0)
        self.assertEqual(8, result[0])
        # self.assertEqual(None, result[1])

        # Test when serial_type is 7
        result = get_record_content(7, test_byte_array, 0)
        self.assertEqual(8, result[0])
        # self.assertEqual(0, result[1])

        # Test when serial_type is 8
        result = get_record_content(8, test_byte_array, 0)
        self.assertEqual(0, result[0])
        self.assertEqual(0, result[1])

        # Test when serial_type is 9
        result = get_record_content(9, test_byte_array, 0)
        self.assertEqual(0, result[0])
        self.assertEqual(1, result[1])

        # Test when serial_type is >= 12 and even
        result = get_record_content(12, test_string_array, 0)
        self.assertEqual(0, result[0])
        self.assertEqual('', result[1])

        result = get_record_content(24, test_string_array, 0)
        self.assertEqual(6, result[0])
        self.assertEqual('this i', result[1])

        # Test when serial_type is >= 13 and odd
        result = get_record_content(13, test_string_array, 0)
        self.assertEqual(0, result[0])
        self.assertEqual('', result[1])

        result = get_record_content(25, test_string_array, 0)
        self.assertEqual(6, result[0])
        self.assertEqual('this i', result[1])

        # Test that the proper exception is thrown when the input is invalid
        cases = [10, 11]
        for case in cases:
            with self.assertRaises(Exception):
                get_record_content(case, test_string_array, 0)

        cases = [3.5, -1, 13.7]
        for case in cases:
            with self.assertRaises(ValueError):
                get_record_content(case, test_string_array, 0)

    def test_get_serial_type_signature(self):
        # Test arguments that don't match and get overridden
        args = [-1, 3.5, 11]
        for arg in args:
            result = get_serial_type_signature(arg)
            self.assertEqual(arg, result)

        # Test arguments that result in a BLOB_SIGNATURE_IDENTIFIER (even numbers >= 12)
        args = [12, 16, 20, 100]
        for arg in args:
            result = get_serial_type_signature(arg)
            self.assertEqual(BLOB_SIGNATURE_IDENTIFIER, result)

        # Test arguments that result in a TEXT_SIGNATURE_IDENTIFIER (odd numbers >= 12)
        args = [13, 17, 21, 101]
        for arg in args:
            result = get_serial_type_signature(arg)
            self.assertEqual(TEXT_SIGNATURE_IDENTIFIER, result)

    def test_has_content(self):
        # Test empty arrays
        cases = [
            bytearray([0x0]),
            bytearray([0x0, 0x0, 0x0]),
            bytearray([0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0])
        ]
        for case in cases:
            result = has_content(case)
            self.assertEqual(False, result)

        # Test non-empty arrays
        cases = [
            bytearray([0x1]),
            bytearray([0x0, 0x1, 0x0]),
            bytearray([0x0, 0x0, 0x0, 0x0, 0x9, 0x0, 0x0, 0x0, 0x2])
        ]
        for case in cases:
            result = has_content(case)
            self.assertEqual(True, result)
