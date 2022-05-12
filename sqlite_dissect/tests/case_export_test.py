import os
import unittest

from sqlite_dissect.entrypoint import main
from sqlite_dissect.utilities import DotDict
from os.path import abspath, join, dirname
from sqlite_dissect.tests.constants import DB_FILES


class TestCASEExport(unittest.TestCase):
    """
    This class tests a parsing of a file and ensuring that it properly generates a CASE export file.
    """

    def test_case_output(self):
        # Get the full path to avoid any nested issues
        base_path = join(dirname(abspath(__file__)), '..', '..')
        input_path = join(DB_FILES, 'chinook.db')
        output_path = join(base_path, 'output')
        case_path = join(output_path, 'case.json')

        # Build the arguments for the testing
        args = {
            'log_level': 'debug',
            'export': ['case', 'text'],
            'directory': output_path,
            'sqlite_file': input_path
        }

        # Convert the dictionary to a dot-accessible object for the main parsing
        args = DotDict(args)

        # Call the main argument
        main(args, input_path)

        # Ensure the case.json file exists
        self.assertTrue(os.path.exists(case_path))
        self.assertTrue(os.path.isfile(case_path))
