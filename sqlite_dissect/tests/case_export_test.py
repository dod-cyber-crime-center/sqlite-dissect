import os
import unittest

from main import main


class DotDict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


class TestCASEExport(unittest.TestCase):
    """
    This class tests a parsing of a file and ensuring that it properly generates a CASE export file.
    """

    # def test_case_output(self):
    #     # Build the arguments for the testing
    #     args = {
    #         'log_level': 'debug',
    #         'export': ['case'],
    #         'directory': '../../output',
    #         'sqlite_file': '../../test_files/chinook.db'
    #     }
    #
    #     # Convert the dictionary to a dot-accessible object for the main parsing
    #     args = DotDict(args)
    #
    #     # Call the main argument
    #     main(args)
    #
    #     # Ensure the case.json file exists
    #     self.assertTrue(os.path.exists('../../output/case.json'))
    #     self.assertTrue(os.path.isfile('../../output/case.json'))
