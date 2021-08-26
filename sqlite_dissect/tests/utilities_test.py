import unittest

from sqlite_dissect.utilities import calculate_expected_overflow


class TestRootUtilities(unittest.TestCase):

    def test_calculate_expected_overflow(self):
        # TODO actually make this an accurate test
        expected = 5
        actual = calculate_expected_overflow(10, 5)
        self.assertEqual(expected, actual)
