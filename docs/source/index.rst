SQLite Dissect Documentation
****************************
SQLite Dissect is a SQLite parser with recovery abilities over SQLite databases and their accompanying journal files. If
no options are set other than the file name, the default behaviour will be to check for any journal files and print to
the console the output of the SQLite files. The directory of the SQLite file specified will be searched through to find
the associated journal files. If they are not in the same directory as the specified file, they will not be found and
their location will need to be specified in the command. SQLite carving will not be done by default. See the full
command line options to enable carving.

.. toctree::
   :maxdepth: 2

   sqlite_dissect/getting_started
   sqlite_dissect/usage
   sqlite_dissect/method_documentation

About
===================
This application focuses on carving by analyzing the allocated content within each of the SQLite database tables and
creating signatures. Where there is no content in the table, the signature is based off of analyzing the create table
statement in the master schema table. The signature contains the series of possible serial types that can be stored
within the file for that table.

This signature is then applied to the unallocated content and freeblocks of the table b-tree in the file. This includes
both interior and leaf table b-tree pages for that table. The signatures are only applied to the pages belonging to the
particular b-tree page it was generated from due to initial research showing that the pages when created or pulled from
the freelist set are overwritten with zeros for the unallocated portions. Fragments within the pages can be reported on
but due to the size (<4 bytes), are not carved. Due to the fact that entries are added into tables in SQLite from the
end of the page and moving toward the beginning, the carving works in the same manner in order to detect previously
partial overwritten entries better. This carving can also be applied to the set of freelist pages within the SQLite file
if specified but the freelist pages are treated as sets of unallocated data currently with the exception of the freelist
page metadata.

The carving process does not currently account for index b-trees as the more pertinent information is included in the
table b-trees. Additionally, there are some table b-trees that are not currently supported. This includes tables that
are "without row_id", virtual, or internal schema objects. These are unique cases which are slightly more rare use cases
or don't offer as much as the main tables do. By default all tables will be carved if they do not fall into one of these
cases. You can send in a specific list of tables to be carved.

This application is written in the hopes that many of these use cases can be addressed in the future and is scalable to
those use cases. Although one specific type of signature is preferred by default in the application, SQLite Dissect
generates multiple versions of a signature and can eventually support carving by specifying other signatures or
providing your own. Since SQLite Dissect generates the signature based off of existing data within the SQLite files
automatically there is no need to supply SQLite Dissect a signature for a particular schema or application. This could
be implemented though to allow possibly more specific/targeted carving of SQLite files through this application.

Journal carving is supported primarily for WAL files. If a WAL file is found, this application will parse through each
of the commit records in sequence and assign a version to them. This is the same as timelining that some applications
use to explain this. Rollback journals are treated as a full unallocated block currently and only support export to csv
files.

SQLite Dissect can support output to various forms: text, csv, xlsx, case, and sqlite. Due to certain constraints on
what can be written to some file types, certain modifications need to be made. For instance, when writing SQLite columns
such as row_id that are already going to pre-exist in the table for export to a SQLite file. In cases like these, we
need to preface the columns with "sd\_" so they will not conflict with the actual row_id column. This also applies to
internal schema objects, so if certain SQLite tables are requested to be written to a SQLite file, than these will be
prefaced with a "iso\_" so they will not conflict with similar internal schema objects that may already exist in the
SQLite file bring written to. In xlsx or csv, due to a "=" symbol indicating a type of equation, these are prefaced with
a " " character to avoid this issue. More details can be found in the code documentation of the export classes
themselves.

SQLite Dissect opens the file as read only and acts as a read only interpreter when parsing and carving the SQLite file.
This is to ensure no changes are made to the files being analyzed. The only use of the sqlite3 libraries in python are
to write the output to a SQLite file if that option is specified for output.
