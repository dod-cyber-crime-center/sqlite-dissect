# DC3 SQLite Dissect
[![SQLite Dissect Python](https://github.com/Defense-Cyber-Crime-Center/sqlite-dissect/actions/workflows/ci.yml/badge.svg)](https://github.com/Defense-Cyber-Crime-Center/sqlite-dissect/actions/workflows/ci.yml)
<a href="https://github.com/Defense-Cyber-Crime-Center/sqlite-dissect/releases" target="_blank"><img src="https://img.shields.io/github/v/release/Defense-Cyber-Crime-Center/sqlite-dissect?label=GitHub%20Release"></a>
<a href="https://pypi.org/project/sqlite-dissect/" target="_blank"><img alt="PyPI" src="https://img.shields.io/pypi/v/sqlite-dissect?label=PyPi%20Release"></a>

### Usage:

    sqlite_dissect [-h] [-v] [-d OUTPUT_DIRECTORY] [-p FILE_PREFIX]
                   [-e EXPORT_TYPE] [-n | -w WAL | -j ROLLBACK_JOURNAL] [-r | EXEMPTED_TABLES]
                   [-s | -t] [-g] [-c] [-f] [-k] [-l LOG_LEVEL] [-i LOG_FILE] [--warnings]
                   SQLITE_PATH

SQLite Dissect is a SQLite parser with recovery abilities over SQLite databases
and their accompanying journal files. If no options are set other than the file
name, the default behaviour will be to check for any journal files and print to
the console the output of the SQLite files.  The directory of the SQLite file
specified will be searched through to find the associated journal files.  If 
they are not in the same directory as the specified file, they will not be found
and their location will need to be specified in the command.  SQLite carving
will not be done by default.  Please see the options below to enable carving.

#### Required Arguments:

| Argument    | Description                                                                                                                                                                                                                  | Example Usage                  |
|-------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------|
| SQLITE_PATH | The path and filename of the SQLite file or directory to be carved. If a directory is provided, it will recursively search for files with the extensions: `.db`, `.sqlite`, `.sqlite3`. | `sqlite_dissect SQLITE_PATH` |  


#### Optional Arguments:

| Argument                         | Flag         | Description                              |
| ---------------------------------|--------------|------------------------------------------|
| --help                           | -h           | show this help message and exit          |
| --version                        | -v           | display the version of SQLite Dissect    |
| --directory DIRECTORY            | -d DIRECTORY | directory to write output to (must be specified for outputs other than console text) |
| --file-prefix PREFIX             | -p PREFIX    | the file prefix to use on output files; default is the name of the SQLite file (the directory for output must be specified) |
| --export FORMATS                 | -e FORMATS   | the format(s) to export to {text, csv, sqlite, xlsx, case}. text written to console if -d is not specified. Multiple space-delimited format values are permitted ex. `-e sqlite csv xlsx`. |
| --no-journal                     | -n           | turn off automatic detection of journal files |
| --wal WAL                        | -w WAL       | the WAL file to use instead of searching the SQLite file directory by default |
| --rollback-journal JOURNAL       | -j JOURNAL   | the rollback journal file to use instead of searching the SQLite file directory by default (under development, currently only outputs to csv, output directory needs to be specified) |
| --exempted-tables TABLES         | -r TABLES    | comma-delimited string of tables \[table1,table2,table3\] to exempt (currently only implemented and allowed for rollback journal parsing) ex. `-r table1,table2,table3`                  |
| --schema                         | -s           | output the the initial schema found in the main database file to console                                                                                                      |
| --schema-history                 | -t           | output the schema history to console, prints the --schema information and write-head log changes                                                                                      |
| --signatures                     | -g           | output the signatures generated to console                                                                                                                                            |
| --carve                          | -c           | carves and recovers table data                                                                                                                                                        |
| --carve-freelists                | -f           | carves freelist pages (carving must be enabled, under development)                                                                                                                    |
| --tables TABLES                  | -b TABLES    | specified comma-delimited string of tables \[table1,table2,table3\] to carve ex. `-b table1,table2,table3`                                                                                |
| --disable-strict-format-checking | -k           | disable strict format checks for SQLite databases (this may result in improperly parsed SQLite files)                                                                                 |
| --log-level LEVEL                | -l LEVEL     | level to log messages at {critical, error, warning, info, debug, off}                                                                                                                 |
| --log-file FILE                  | -i FILE      | log file to write to; appends to file if file already exists. default is to write to console. ignored if log-level set to `off`                                                               |
| --warnings                       |              | enable runtime warnings                                                                                                                                                               |
 | --header                         |              | enable header info printing                                                                                                                                                           |
 | --config FILE                         |              | file containing configuration values for the execution of SQLite Dissect                                                                                                                                                            |

### Command Line Usage:

1. Print the version:

```shell
sqlite_dissect --version
```

2. Parse a SQLite database and print the outputs to the screen:

```shell
sqlite_dissect [SQLITE_PATH]
```


3. Parse a SQLite database and print schema history to a SQLite output file:

```shell
sqlite_dissect [SQLITE_PATH] --schema-history -d [OUTPUT_DIRECTORY] -e sqlite
```

4. Parse a SQLite database and print the output to a SQLite file along with printing signatures and carving entries:

```shell
sqlite_dissect [SQLITE_PATH] --signatures -d [OUTPUT_DIRECTORY] -e sqlite --carve
```

5. Parse a SQLite database and print the output to a SQLite file and carving entries, including freelists, for specific tables:

```shell
sqlite_dissect [SQLITE_PATH] -d [OUTPUT_DIRECTORY] -e sqlite --carve --carve-freelists -b [TABLES]
```

6. Parse a SQLite database file and print the output to a xlsx workbook along with generating signatures and 
   carving entries.  The schema history (schema updates throughout the WAL are included if a WAL file is detected) and 
   signatures will be printed to standard output.  The log level will be set to debug and all log messages will be
   output to the specified log file.

```shell
sqlite_dissect [SQLITE_PATH] -d [OUTPUT_DIRECTORY] -e xlsx --schema-history --carve --signatures --log-level debug -i [LOG_FILE]
```

7. Parse a SQLite database file along with a specified rollback journal file and send the output to CSV files.  
   (CSV is the only output option currently implemented for rollback journal files)
   
```shell
sqlite_dissect [SQLITE_PATH] -d [OUTPUT_DIRECTORY] -e csv --carve -j [ROLLBACK_JOURNAL]
```

##### Configuration Files
SQLite Dissect can optionally be configured with configuration files that are provided using the CLI argument `--config`

The format for the configuration file is as follows:
```text
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
```

For example:
```config
[export]
directory=/path/to/output
export=[text, sqlite, case]
```

##### Environment Variables
SQLite Dissect can also be configured using environment variables with the prefixed version of the argument flag (SQLD_).

For example:
```shell
export SQLD_DIRECTORY=/path/to/output
export SQLD_EXPORT_TYPE="[text, sqlite, case]"
```

### Description

This application focuses on carving by analyzing the allocated content within each of the SQLite
database tables and creating signatures.  Where there is no content in the table, the signature
is based off of analyzing the create table statement in the master schema table.  The signature
contains the series of possible serial types that can be stored within the file for that table.  
This signature is then applied to the unallocated content and freeblocks of the table b-tree in
the file.  This includes both interior and leaf table b-tree pages for that table.  The signatures 
are only applied to the pages belonging to the particular b-tree page it was generated from due
to initial research showing that the pages when created or pulled from the freelist set are
overwritten with zeros for the unallocated portions.  Fragments within the pages can be reported
on but, due to the size (<4 bytes), are not carved.  Due to the fact that entries are added into
tables in SQLite from the end of the page and moving toward the beginning, the carving works
in the same manner in order to detect previously partially overwritten entries better.  This 
carving can also be applied to the set of freelist pages within the SQLite file if specified
but the freelist pages are currently treated as sets of unallocated data with the exception 
of the freelist page metadata.

The carving process does not currently account for index b-trees as the more pertinent information
is included in the table b-trees.  Additionally, there are some table b-trees that are not currently
supported.  This includes tables that are "without row_id", virtual, or internal schema objects.
These are unique cases which are slightly more rare use cases or don't offer as much as the
main tables do.  By default all tables will be carved if they do not fall into one of these cases.
You can send in a specific list of tables to be carved.

This application is written in the hopes that many of these use cases can be addressed in the future
and is scalable to those use cases.  Although one specific type of signature is preferred by default
in the application, SQLite Dissect generates multiple versions of a signature and can eventually
support carving by specifying other signatures or providing your own.  Since SQLite Dissect generates
the signature based off of existing data within the SQLite files automatically, there is no need to
supply SQLite Dissect a signature for a particular schema or application.  This could be implemented
though to allow possibly more specific/targeted carving of SQLite files through this application.

Journal carving is supported primarily for WAL files.  If a WAL file is found, this application will
parse through each of the commit records in sequence and assign a version to them.  This is the same
as timelining that some applications use to explain this.  Rollback journals are currentlytreated as
a full unallocated block and only support export to csv files.

SQLite Dissect can support output to various forms: text, csv, xlsx, and sqlite.  Due to certain
constraints on what can be written to some file types, certain modifications need to be made.  For
instance, when writing SQLite columns such as row_id that are already going to pre-exist in the table
for export to a SQLite file we need to preface the columns with "sd_" so they will not conflict with 
the actual row_id column.  This also applies to internal schema objects. If certain SQLite tables are 
requested to be written to a SQLite file, than these will be prefaced with "iso_" so they will not 
conflict with similar internal schema objects that may already exist in the SQLite file bring written 
to.  In xlsx or csv, due to a "=" symbol indicating a type of equation, these are prefaced with a " " 
character to avoid this issue.  More details can be found in the code documentation of the export classes 
themselves.

SQLite Dissect opens the file as read only and acts as a read only interpreter when parsing and carving
the SQLite file.  This is to ensure no changes are made to the files being analyzed.  The only use
of the sqlite3 libraries in Python are to write the output to a SQLite file if that option is
specified for output.

#### Additional Notes:
1. SQLite Dissect currently only works on a SQLite database or a SQLite database along with a journal
   (WAL or rollback) file.  Journal files by themselves are not supported yet.

#### Currently not implemented:
1. Signatures and carving are not implemented for "without rowid" tables or indexes.  This will not cause an error 
   but will skip signature generation and carving processes.
2. Signatures and carving are not implemented for virtual tables.  This will not cause an error but will skip 
   signature generation and carving processes.  `Note:  Even though virtual tables are skipped, virtual tables may 
   create other non-virtual tables which are not skipped.  Currently nothing ties these tables back to the virtual
   table that created them.`
3. Invalidated frames in WAL files are currently skipped and not parsed.  `Note:  This applies to previous WAL records
   that were previously written to the SQLite database.`
4. Signatures generated are only reflective of the base/initial schema in the SQLite database.

#### Known issues and errors:
1. A use case may occur on generating a very small signature due to a table with very few columns resulting in
   many false positives and longer parsing time.
2. Due to current handling queuing of data objects to be printed in addition to #1 above, a memory issue may
   occur with carving some tables.

#### Future implementation:
1. Export binary objects to separate files during export instead of being written to text files.
2. Print out sets of data that were unallocated or in freeblocks that did not have successful carvings.
3. Fix issues with schemas with comments.
4. Handle "altered column" table signatures where detected.
5. Implement handling of invalidated WAL frames.
6. The ability to de-dupe carved entries to those in allocated space (in cases such as those where the b-tree was migrated).

# Library Scripts

High level scripts that are used to access the rest of the library and provide the base application for executing
SQLite Dissect when built.

- api_usage.py
- example.py
- setup.py
- sqlite_dissect.py

<br>

### api_usage.py

This script shows an example of the api usage for a specific test file.

TODO:
- [ ] Documentation improvements.

<br>

### example.py

This script shows examples of how this library can be used.

TODO:
- [ ] Documentation improvements.
- [ ] Implement additional export methods.

<br>

### setup.py

This script is used to setup the sqlite_dissect package for use in python environments.

>Note:  To compile a distribution for the project run "python setup.py sdist" in the directory this file is located in.

>Note: openpyxl is needed for the xlsx export and will install jdcal, et-xmlfile \["openpyxl>=2.4.0b1"\]

>Note: PyInstaller is used for generation of executables but not included in this setup.py script and will
>      install altgraph, dis3, macholib, pefile, pypiwin32, pywin32 as dependencies. \[pyinstaller==3.6 needs to be used
>      for Python 2.7 since the newer versions of PyInstaller of 4.0+ require Python 3.6\]  Information on how to run
>      PyInstaller is included in the spec files under the pyinstaller directory.  Four files are here, two for windows
>      and two for linux, both for x64 platforms.  The two different files for each allow you to build it as one single
>      file or a directory of decompressed files.  Since the one file extracts to a temp directory in order to run, on
>      some systems this may be blocked and therefore the directory of files is preferred.

<br>

### sqlite_dissect.py

This script acts as the command line script to run this library as a stand-alone application.

TODO:
- [ ] Documentation improvements.
- [ ] Implement append, overwrite, etc. options for the log file if specified.
- [ ] Incorporate signature generation input and output files once implemented.
- [ ] Incorporate "store in memory" arguments (currently set to False, more in depth operations may want it True).
- [ ] Implement multiple passes/depths.
- [ ] Test use cases for exempted tables with rollback journal and when combined with specified tables.  
- [ ] Check on name vs table_name properties of the master schema entry.  
- [ ] Test cases where the schema changes throughout the WAL file.
- [ ] Investigate handling of virtual and "without rowid" tables when creating table signatures through the interface.
- [ ] Documentation on "without rowid" tables and indexes in references to carving in help documentation.
- [ ] Make sure to address/print unallocated space (especially uncarved) from updated page numbers in commit records.
- [ ] Research if there can be journal files with a zero length database file or zero-length journal files.
- [ ] Research if there can be combinations and of multiple rollback journal and WAL files with the SQLite database.
- [ ] Validate initial research that allocation of freelist pages to a b-tree results in a wipe of the page data.
- [ ] Add additional logging messages to the master schema entries skipped in signature generation. 
- [ ] Integrate in the SQLite Forensic Corpus into tests.
- [ ] Look into updating terminology for versioning to timelining.
- [ ] Update code for compatibility with Python 3.
- [ ] Create PyUnit tests.
- [ ] Create a GUI.
