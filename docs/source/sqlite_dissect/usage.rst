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

    # When installed as a binary or package
    sqlite_dissect /path/to/sqlite.db

    # When running the Python file directly
    python entrypoint.py /path/to/sqlite.db

All Optional CLI Arguments
++++++++++++++++++++++++++

.. list-table::
    :widths: 25 25 50
    :header-rows: 1

    * - Argument
      - Flag
      - Description
    * - --help
      - -h
      - show this help message and exit
    * - --version
      - -v
      - display the version of SQLite Dissect
    * - --directory DIRECTORY
      - -d DIRECTORY
      - directory to write output to (must be specified for outputs other than console text)
    * - --file-prefix PREFIX
      - -p PREFIX
      - the file prefix to use on output files, default is the name of the SQLite file (the directory for output must be specified)
    * - --export FORMATS
      - -e FORMATS
      - the format(s) to export to {text, csv, sqlite, xlsx, case} (text written to console if -d is not specified). Multiple space-delimited formats are permitted eg -e sqlite csv xlsx.
    * - --no-journal
      - -n
      - turn off automatic detection of journal files
    * - --wal WAL
      - -w WAL
      - the WAL file to use instead of searching the SQLite file directory by default
    * - --rollback-journal JOURNAL
      - -j JOURNAL
      - the rollback journal file to use instead of searching the SQLite file directory by default (under development, currently only outputs to csv, output directory needs to be specified)
    * - --exempted-tables TABLES
      - -r TABLES
      - comma-delimited string of tables [table1,table2,table3] to exempt (only implemented and allowed for rollback journal parsing currently) ex.) table1,table2,table3
    * - --schema
      - -s
      - output the schema to console, the initial schema found in the main database file
    * - --schema-history
      - -t
      - output the schema history to console, prints the --schema information and write-head log changes
    * - --signatures
      - -g
      - output the signatures generated to console
    * - --carve
      - -c
      - carves and recovers table data
    * - --carve-freelists
      - -f
      - carves freelist pages (carving must be enabled, under development)
    * - --tables TABLES
      - -b TABLES
      - specified comma-delimited string of tables [table1,table2,table3] to carve ex.) table1,table2,table3
    * - --disable-strict-format-checking
      - -k
      - disable strict format checks for SQLite databases (this may result in improperly parsed SQLite files)
    * - --log-level LEVEL
      - -l LEVEL
      - level to log messages at {critical, error, warning, info, debug, off}
    * - --log-file FILE
      - -i FILE
      - log file to write too, default is to write to console, ignored if log level set to off (appends if file already exists)
    * - --warnings
      -
      - enable runtime warnings

Command Line Configuration
+++++++++++++++++++++++++++
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

File-Based Configuration
++++++++++++++++++++++++++
SQLite Dissect can optionally be configured with configuration files that are provided using the CLI argument `--config`

The format for the configuration file is as follows:

.. code-block:: shell

    # this is a comment
    ; this is also a comment (.ini style)
    ---            # lines that start with --- are ignored (yaml style)
    -------------------
    [section]      # .ini-style section names are treated as comments

    # how to specify a key-value pair (all of these are equivalent):
    name value     # key is case sensitive: "Name" isn't "name"
    name = value   # (.ini style)  (white space is ignored, so name = value same as name=value)
    name: value    # (yaml style)
    --name value   # (argparse style)

    # how to set a flag arg (eg. arg which has action="store_true")
    --name
    name
    name = True    # "True" and "true" are the same

    # how to specify a list arg (eg. arg which has action="append")
    fruit = [apple, orange, lemon]
    indexes = [1, 12, 35 , 40]


For example:

.. code-block:: shell

    [export]
    directory=/path/to/output
    export=[text, sqlite, case]

Environment Variable Configuration
++++++++++++++++++++++++++++++++++++
SQLite Dissect can also be configured using environment variables with the prefixed version of the argument flag.

For example:

.. code-block:: shell

    export SQLD_DIRECTORY=/path/to/output
    export SQLD_EXPORT_TYPE="[text, sqlite, case]"
