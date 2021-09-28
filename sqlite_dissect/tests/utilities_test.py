import unittest

from sqlite_dissect.utilities import calculate_expected_overflow, get_serial_type_signature
from sqlite_dissect.constants import STORAGE_CLASS, BLOB_SIGNATURE_IDENTIFIER, TEXT_SIGNATURE_IDENTIFIER


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
      
      
    def test_get_serial_type_signature(self):
        # Test arguments that don't match and get overridden
        args = [-1, 3.5, 25]
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
          