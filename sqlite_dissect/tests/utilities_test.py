import unittest

from sqlite_dissect.utilities import calculate_expected_overflow
from sqlite_dissect.constants import STORAGE_CLASS


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
      
      
    def test_get_storage_class(self):
        # This function is pretty straightforward, so tests are mostly spot checks to ensure there are
        # no breaking changes made, and not focusing on replicating the exact logic that already exists
        # in the function itself.
        
        # Test invalid
        checks = [-1, 3.5, 13, 25]
        for cls in checks:
          result = get_storage_class(cls)
          self.assertEqual(None, result)
        
        # Test NULL
        result = get_storage_class(0)
        self.assertEqual(STORAGE_CLASS.NULL, result)
        
        # Test REAL
        result = get_storage_class(7)
        self.assertEqual(STORAGE_CLASS.REAL, result)

        # Test INTEGER
        checks = [1, 2, 3, 4, 5, 6, 8, 9]
        for cls in checks:
          result = get_storage_class(cls)
          self.assertEqual(STORAGE_CLASS.INTEGER, result)
          
        # Test BLOB
        checks = [12, 14, 16, 100]
        for cls in checks:
          result = get_storage_class(cls)
          self.assertEqual(STORAGE_CLASS.BLOB, result)  
          
        # Test TEXT
        checks = [14, 16, 100]
        for cls in checks:
          result = get_storage_class(cls)
          self.assertEqual(STORAGE_CLASS.TEXT, result)
