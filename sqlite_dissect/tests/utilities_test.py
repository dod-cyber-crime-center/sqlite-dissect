import unittest

from sqlite_dissect.utilities import calculate_expected_overflow


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
