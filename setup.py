import os
from setuptools import setup

"""

setup.py

This script will be used to setup the sqlite_dissect package for use in python environments.

Note:  To compile a distribution for the project run "python setup.py sdist" in the directory this file is located in.
To also build the wheel, run "python setup.py sdist bdist_wheel".

Note: openpyxl is needed for the xlsx export and will install jdcal and et-xmlfile ["openpyxl>=2.4.0b1"]

Note: PyInstaller is used for generation of executables but not included in this setup.py script and will
      install altgraph, dis3, macholib, pefile, pypiwin32, pywin32 as dependencies. [pyinstaller==3.6 needs to be used
      for Python 2.7 since the newer versions of PyInstaller of 4.0+ require Python 3.6]  Information on how to run
      PyInstaller is included in the spec files under the pyinstaller directory.  Four files are here, two for windows
      and two for linux, both for x64 platforms.  The two different files for each allow you to build it as one single
      file or a directory of decompressed files.  Since the one file extracts to a temp directory in order to run, on
      some systems this may be blocked and therefore the directory of files is preferred.

"""

# Imports the __version__ since the package references don't yet exist in the scope of setup.py. It opens the file
# and interprets the code so the __version__ variable is populated, despite IDE warnings that it's undefined.
exec (open('sqlite_dissect/_version.py').read())

# The text of the README file
README = open(os.path.join(os.path.dirname(__file__), 'README.md')).read()

setup(name="sqlite_dissect",
      version=os.environ.get('TAG_VERSION', __version__),  # noqa: F821
      url="https://github.com/Defense-Cyber-Crime-Center/sqlite-dissect",
      description="This package allows parsing and carving of SQLite files",
      long_description=README,
      long_description_content_type="text/markdown",
      author="Defense Cyber Crime Center (DC3)",
      author_email="TSD@dc3.mil",
      packages=["sqlite_dissect",
                "sqlite_dissect.file",
                "sqlite_dissect.file.database",
                "sqlite_dissect.file.journal",
                "sqlite_dissect.file.schema",
                "sqlite_dissect.file.wal",
                "sqlite_dissect.file.wal_index",
                "sqlite_dissect.carving",
                "sqlite_dissect.export"],
      classifiers=[
          "Programming Language :: Python :: 2",
          "Programming Language :: Python :: 2.7"
      ],
      entry_points={
          'console_scripts': ['sqlite_dissect=sqlite_dissect.entrypoint:cli'],
      },
      install_requires=[
          "openpyxl==2.6.4",
          "ConfigArgParse"
      ],
      zip_safe=False
      )
