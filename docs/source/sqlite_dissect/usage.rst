Usage
===================

.. code-block:: shell

    sqlite_dissect [-h] [-v] [-d OUTPUT_DIRECTORY] [-p FILE_PREFIX]
               [-e EXPORT_TYPE] [-n | -w WAL | -j ROLLBACK_JOURNAL] [-r | EXEMPTED_TABLES]
               [-s | -t] [-g] [-c] [-f] [-k] [-l LOG_LEVEL] [-i LOG_FILE] [--warnings]
               SQLITE_FILE

Basic CLI Usage
+++++++++++++++++++

.. code-block:: shell

    sqlite_dissect /path/to/sqlite.db

All CLI Arguments
+++++++++++++++++++

.. list-table::
    :widths: 25 25 50
    :header-rows: 1

    * - Argument
      - Flag
      - Description
    * - Row 1, column 1
      -
      - Row 1, column 3
    * - Row 2, column 1
      - Row 2, column 2
      - Row 2, column 3

Example Usage
+++++++++++++++++++
Print the version:

.. code-block:: shell

    sqlite_dissect --version

Parse a SQLite database and print the outputs to the screen:

.. code-block:: shell

    sqlite_dissect [SQLITE_FILE]

Parse a SQLite database and print schema history to a SQLite output file:

.. code-block:: shell

    sqlite_dissect [SQLITE_FILE] \
            --schema-history \
            -d [OUTPUT_DIRECTORY] \
            -e sqlite

Parse a SQLite database and print the output to a SQLite file along with printing signatures and carving entries:

.. code-block:: shell

    sqlite_dissect [SQLITE_FILE] \
            --signatures \
            -d [OUTPUT_DIRECTORY] \
            -e sqlite \
            --carve

Parse a SQLite database and print the output to a SQLite file and carving entries, including freelists, for specific tables:

.. code-block:: shell

    sqlite_dissect [SQLITE_FILE] \
            -d [OUTPUT_DIRECTORY] \
            -e sqlite \
            --carve \
            --carve-freelists \
            -b [TABLES]

Parse a SQLite database file and print the output to a xlsx workbook along with generating signatures and carving entries. The schema history (schema updates throughout the WAL included if a WAL file detected) and signatures will be printed to standard output. The log level will be set to debug and all log messages will be output to the specified log file.

.. code-block:: shell

    sqlite_dissect [SQLITE_FILE] \
            -d [OUTPUT_DIRECTORY] \
            -e xlsx --schema-history \
            --carve \
            --signatures \
            --log-level debug \
            -i [LOG_FILE]

Parse a SQLite database file along with a specified rollback journal file and send the output to CSV files.
(CSV is the only output option currently implemented for rollback journal files.)

.. code-block:: shell

    sqlite_dissect [SQLITE_FILE] \
            -d [OUTPUT_DIRECTORY] \
            -e csv \
            --carve \
            -j [ROLLBACK_JOURNAL]

