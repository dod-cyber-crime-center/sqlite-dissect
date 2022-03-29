import os
import unittest
from argparse import Namespace

from main import main
from os.path import abspath, join, realpath, dirname


class TestCASEExport(unittest.TestCase):
    """
    This class tests a parsing of a file and ensuring that it properly generates a CASE export file.
    """

    def test_case_output(self):
        # Get the full path to avoid any nested issues
        base_path = abspath(join(dirname(realpath(__file__)), '..', '..'))
        input_path = join(base_path, 'test_files', 'chinook.db')
        output_path = join(base_path, 'output')
        case_path = join(output_path, 'case.json')

        # Build the arguments for the testing
        args = Namespace()
        args.log_level = 'debug'
        args.export = ['case']
        args.directory = output_path
        args.sqlite_file = input_path

        # Call the main argument
        main(args, input_path)

        # Ensure the case.json file exists
        self.assertTrue(os.path.exists(case_path))
        self.assertTrue(os.path.isfile(case_path))
