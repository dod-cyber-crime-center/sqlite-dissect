import os
import unittest

from main import main
from sqlite_dissect.utilities import DotDict


class TestCASEExport(unittest.TestCase):
    """
    This class tests a parsing of a file and ensuring that it properly generates a CASE export file.
    """

    def test_case_output(self):
        # Build the arguments for the testing
        args = {
            'log_level': 'debug',
            'export': ['case'],
            'directory': 'output',
            'sqlite_file': 'test_files/chinook.db'
        }

        # Convert the dictionary to a dot-accessible object for the main parsing
        args = DotDict(args)

        # Call the main argument
        main(args)

        # Ensure the case.json file exists
        self.assertTrue(os.path.exists('output/case.json'))
        self.assertTrue(os.path.isfile('output/case.json'))

        # TODO add a validator for the exported case.json file based on the project at:
        # https://github.com/ucoProject/UCO-Utility-Pre-0.7.0-Validator
        # Once the environment is setup as specified above, the following command can be run to validate the output
        # `validate case-0.4.0.pkl path/to/case.json`
