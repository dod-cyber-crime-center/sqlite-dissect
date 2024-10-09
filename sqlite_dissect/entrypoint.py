import uuid
import warnings
import sys
from logging import CRITICAL, ERROR, WARNING, INFO, DEBUG, basicConfig, getLogger, StreamHandler, FileHandler
from os import path
from os.path import basename, abspath, join, exists, getsize, normpath, sep
from time import time
from warnings import warn
from sqlite_dissect.carving.rollback_journal_carver import RollBackJournalCarver
from sqlite_dissect.carving.signature import Signature
from sqlite_dissect.constants import BASE_VERSION_NUMBER
from sqlite_dissect.constants import EXPORT_TYPES
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import MASTER_SCHEMA_ROW_TYPE
from sqlite_dissect.constants import ROLLBACK_JOURNAL_POSTFIX
from sqlite_dissect.constants import WAL_FILE_POSTFIX
from sqlite_dissect.exception import SqliteError
from sqlite_dissect.export.case_export import CaseExporter
from sqlite_dissect.export.csv_export import CommitCsvExporter
from sqlite_dissect.export.sqlite_export import CommitSqliteExporter
from sqlite_dissect.export.text_export import CommitConsoleExporter
from sqlite_dissect.export.text_export import CommitTextExporter
from sqlite_dissect.export.xlsx_export import CommitXlsxExporter
from sqlite_dissect.file.database.database import Database
from sqlite_dissect.file.journal.jounal import RollbackJournal
from sqlite_dissect.file.schema.master import OrdinaryTableRow
from sqlite_dissect.file.wal.wal import WriteAheadLog
from sqlite_dissect.output import stringify_master_schema_version
from sqlite_dissect.output import stringify_master_schema_versions
from sqlite_dissect.utilities import get_sqlite_files, create_directory, parse_args
from sqlite_dissect.version_history import VersionHistory
from sqlite_dissect.version_history import VersionHistoryParser
from datetime import datetime

"""

sqlite_dissect.py

This script will act as the command line script to run this library as a stand-alone application.

"""


def main(arguments, sqlite_file_path: str, export_sub_paths=False):
    """
    The primary entrypoint for the SQLite Dissect carving utility.

    :param arguments: the object of key => value arguments that have been provided as the runtime configuration in which
        the SQLite Dissect tool should run.
    :param sqlite_file_path: the string path to the SQLite file or directory being processed.
    :param export_sub_paths: the boolean determination of whether to generate subdirectories for the output for each
        SQLite file being processed.
    """

    # set program output type to UTF-8
    if 'pytest' not in sys.modules:
        sys.stdout.reconfigure(encoding='utf-8', errors='namereplace')

    # Handle the logging and warning settings
    if not arguments.log_level:
        raise SqliteError("Error in setting up logging: no log level determined.")

    # Get the logging level
    logger = getLogger(LOGGER_NAME)
    logging_level_arg = arguments.log_level
    logging_level = logging_level_arg
    if logging_level_arg != "off":
        if logging_level_arg == "critical":
            logging_level = CRITICAL
        elif logging_level_arg == "error":
            logging_level = ERROR
        elif logging_level_arg == "warning":
            logging_level = WARNING
        elif logging_level_arg == "info":
            logging_level = INFO
        elif logging_level_arg == "debug":
            logging_level = DEBUG
        else:
            raise SqliteError("Invalid option for logging: {}.".format(logging_level_arg))
    else:
        logging_level = None

    # Setup logging
    basicConfig(level=logging_level,
                format='%(levelname)s %(asctime)s [%(pathname)s] %(funcName)s at line %(lineno)d: %(message)s',
                datefmt='%d %b %Y %H:%M:%S',
                filename=arguments.log_file if arguments.log_file else None)

    logger.debug(f"Setup logging using the log level: {logging_level}.")
    logger.info(f"Using options: {arguments}")

    case = CaseExporter(logger)
    case.start_datetime = datetime.now()

    if arguments.warnings:

        # Turn warnings on if it was specified
        warnings.filterwarnings("always")

        logger.info("Warnings have been turned on.")

    else:

        # Ignore warnings by default
        warnings.filterwarnings("ignore")

    # Execute argument checks (inclusive)
    if arguments.carve_freelists and not arguments.carve:
        raise SqliteError("Freelist carving cannot be enabled (--carve-freelists) without enabling "
                          "general carving (--carve).")
    # If there is an export format specified that is not "text", then an output directory is required. It is assumed
    # that if there is more than one output format specified then at least one of them is not "text". If there is only
    # one, then it's checked against the "text" format
    if arguments.export and (len(arguments.export) > 1 or (len(arguments.export) == 1 and
            arguments.export[0].upper() != EXPORT_TYPES.TEXT)) and not arguments.directory:
        raise SqliteError("The directory needs to be specified (--directory) if an export type other than text "
                          "is specified (--export).")
    if arguments.file_prefix and not arguments.directory:
        raise SqliteError("The directory needs to be specified (--directory) if a file prefix is "
                          "specified (--file-prefix).")

    # Setup the export type
    export_types = [EXPORT_TYPES.TEXT]
    if arguments.export and len(export_types) > 0:
        export_types = list(map(str.upper, arguments.export))

    # Setup the strict format checking
    strict_format_checking = True
    if arguments.disable_strict_format_checking:
        strict_format_checking = False

    # Setup the file prefix which taken from the base version name unless the file_prefix argument is set
    file_prefix = basename(normpath(sqlite_file_path))
    if arguments.file_prefix:
        file_prefix = arguments.file_prefix

    if not file_prefix:
        # The file prefix is taken from the base version name if not specified
        file_prefix = basename(normpath(sqlite_file_path))

    # Setup the directory if specified
    output_directory = None
    if arguments.directory:
        if not exists(arguments.directory):
            if not create_directory(arguments.directory):
                raise IOError("Unable to create the new output directory: {}", arguments.directory)
        output_directory = arguments.directory
        # Determine if there are sub-paths being configured for exports
        if export_sub_paths:
            # Generate unique subpath and create the directory
            subpath = file_prefix.replace(".", "-") + "-" + str(uuid.uuid4().hex)
            if create_directory(join(output_directory, subpath)):
                output_directory = join(output_directory, subpath)
            else:
                raise IOError("Unable to create the new sub-directory: {}", join(output_directory, subpath))

    logger.debug(
        f"Determined export type to be {export_types} with file prefix: {file_prefix} and output directory: {output_directory}")

    # Obtain the SQLite file
    if not exists(sqlite_file_path):
        raise SqliteError(f"Unable to find SQLite file: {sqlite_file_path}.")

    """

    If the file is a zero length file, we set a flag indicating it and check to make sure there are no associated wal
    or journal files before just exiting out stating that the file was empty.  If a (non-zero length) wal or journal
    file is found, an exception will be thrown.  However, if the no-journal option is specified, the journal files will
    not be checked, and the program will exit.

    Note:  It is currently believed that there cannot be a zero length SQLite database file with a wal or journal file.
           That is why an exception is thrown here but needs to be investigated to make sure.

    """

    # See if the SQLite file is zero-length
    zero_length_sqlite_file = False
    if getsize(sqlite_file_path) == 0:
        zero_length_sqlite_file = True

    # Obtain the wal or rollback_journal file if found (or if specified)
    wal_file_name = None
    rollback_journal_file_name = None
    if not arguments.no_journal:
        if arguments.wal:
            if not exists(arguments.wal):
                raise SqliteError(f"Unable to find wal file: {arguments.wal}.")
            wal_file_name = arguments.wal
        elif arguments.rollback_journal:
            if not exists(arguments.rollback_journal):
                raise SqliteError(f"Unable to find rollback journal file: {arguments.rollback_journal}.")
            rollback_journal_file_name = arguments.rollback_journal
        else:
            if exists(sqlite_file_path + WAL_FILE_POSTFIX):
                wal_file_name = sqlite_file_path + WAL_FILE_POSTFIX
            if exists(sqlite_file_path + ROLLBACK_JOURNAL_POSTFIX):
                rollback_journal_file_name = sqlite_file_path + ROLLBACK_JOURNAL_POSTFIX

    # Exempted tables are only supported currently for rollback journal files
    rollback_journal_exempted_tables = []
    if arguments.exempted_tables:
        if not rollback_journal_file_name:
            raise SqliteError("Exempted tables are only supported for use with rollback journal parsing.")
        rollback_journal_exempted_tables = arguments.exempted_tables.split(",")

    # See if the wal file is zero-length
    zero_length_wal_file = False
    if wal_file_name and getsize(wal_file_name) == 0:
        zero_length_wal_file = True

    # See if the rollback journal file is zero-length
    zero_length_rollback_journal_file = False
    if rollback_journal_file_name and getsize(rollback_journal_file_name) == 0:
        zero_length_rollback_journal_file = True

    # Check if the SQLite file is zero length
    if zero_length_sqlite_file:

        if wal_file_name and not zero_length_wal_file:

            """

            Here we throw an exception if we find a wal file with content with no content in the original SQLite file.
            It is not certain this use case can occur and investigation needs to be done to make certain.  There have
            been scenarios where there will be a database header with no schema or content in a database file with a
            WAL file that has all the schema entries and content but this is handled differently.

            """

            raise SqliteError(f"Found a zero length SQLite file with a wal file: {arguments.wal}.  Unable to parse.")

        elif zero_length_wal_file:
            logger.error(f"File: {sqlite_file_path} with wal file: {wal_file_name} has no content.  Nothing to parse.")
            exit(0)

        elif rollback_journal_file_name and not zero_length_rollback_journal_file:

            """

            Here we will only have a rollback journal file.  Currently, since we need to have the database file to parse
            signatures from, we cannot solely carve on the journal file alone.

            """

            raise SqliteError(
                f"Found a zero length SQLite file with a rollback journal file: {arguments.rollback_journal}.  "
                f"Unable to parse.")

        elif zero_length_rollback_journal_file:
            logger.error(
                f"File: {sqlite_file_path} with rollback journal file: {rollback_journal_file_name} has no content. "
                f"Nothing to parse.")
            exit(0)

        else:
            logger.error("File: {} has no content. Nothing to parse.".format(sqlite_file_path))
            exit(0)

    # Make sure that both of the journal files are not found
    if rollback_journal_file_name and wal_file_name:
        """

        Since the arguments have you specify the journal file in a way that you can only set the wal or rollback journal
        file name, this case can only occur from finding both of the files on the file system for both wal and rollback
        journal when there is no journal options specified.  Since the SQLite database cannot be set to use both wal and
        journal files in the same running, we determine this to be an error and throw and exception up.

        There may be a case where the mode was changed at some point and there is a single SQLite file with one or more
        journal files in combination of rollback journal and WAL files.  More research would have to take place in this
        scenario and also take into the account of this actually occurring since in most cases it is set statically
        by the application SQLite database owner.

        """

        raise SqliteError(
            f"Found both a rollback journal: {arguments.rollback_journal} and wal file: {arguments.wal}.  "
            f"Only one journal file should exist. Unable to parse.")

    # Print a message parsing is starting and log the start time for reporting at the end on amount of time to run
    logger.info(f"\nParsing: {sqlite_file_path}...")
    start_time = time()

    # Create the database and wal/rollback journal file (if existent)
    database = Database(sqlite_file_path, strict_format_checking=strict_format_checking)

    write_ahead_log = None
    if wal_file_name and not zero_length_wal_file:
        write_ahead_log = WriteAheadLog(wal_file_name, strict_format_checking=strict_format_checking)

    rollback_journal_file = None
    if rollback_journal_file_name and not zero_length_rollback_journal_file:
        rollback_journal_file = RollbackJournal(rollback_journal_file_name)

    # Create the version history (this is currently only supported for the WAL)
    version_history = VersionHistory(database, write_ahead_log)

    printable_header = database.database_header.stringify(padding="\t")
    logger.debug(f"\nDatabase header information:\n{printable_header}")
    logger.debug("Continuing to parse...")
    # Check if the header info was asked for
    if arguments.header:
        # Print the header info of the database
        print(f"\nDatabase header information:\n{printable_header}")
        print("Continuing to parse...")

    printable_schema = stringify_master_schema_version(database)
    logger.debug(f"\nDatabase Master Schema:\n{printable_schema}")
    logger.debug("Continuing to parse...")
    # Check if the master schema was asked for
    if arguments.schema:
        # print the master schema of the database
        print(f"\nDatabase Master Schema:\n{printable_schema}")
        print("Continuing to parse...")

    printable_schema_history = stringify_master_schema_versions(version_history)
    logger.debug(f"\nVersion History of Master Schemas:\n{printable_schema_history}")
    logger.debug("Continuing to parse...")
    # Check if the schema history was asked for
    if arguments.schema_history:
        # print the master schema version history
        print(f"\nVersion History of Master Schemas:\n{printable_schema_history}")
        print("Continuing to parse...")

    # Get the signature options
    print_signatures = arguments.signatures

    # Get the carving options
    carve = arguments.carve
    carve_freelists = arguments.carve_freelists

    # Check to see if carve freelists was set without setting carve
    if not carve and carve_freelists:
        log_message = "The carve option was not set but the carve_freelists option was.  Disabling carve_freelists.  " \
                      "Please specify the carve option to enable."
        logger.warning(log_message)
        warn(log_message, RuntimeWarning)

    # Specific tables to be carved
    specified_tables_to_carve = []
    if arguments.tables:
        specified_tables_to_carve = arguments.tables.split(",")

    # TODO: if code no longer necessary, delete
    # if rollback_journal_exempted_tables and specified_tables_to_carve:
    #    for table in rollback_journal_exempted_tables:
    #        if table in specified_tables_to_carve:
    #            logger.error(f"Table: {table} found in both exempted and specified tables.  Please update the "
    #                         f"arguments correctly.")
    #            exit(0)

    # See if we need to generate signatures
    generate_signatures = True if (carve or print_signatures) else False
    signatures = None

    # Get all of the signatures (for tables only - not including "without rowid" and virtual tables)
    if generate_signatures:

        signatures = {}
        logger.debug("Generating table signatures.")

        for master_schema_entry in database.master_schema.master_schema_entries:

            # Only account for the specified tables
            if specified_tables_to_carve and master_schema_entry.name not in specified_tables_to_carve:
                continue

            """

            Due to current implementation limitations we are restricting carving to table row types.

            Note:  This is not allowing "without rowid" or virtual tables until further testing is done.
                   (Virtual tables tend to have a root page number of 0 with no data stored in the main table.  Further
                   investigation is needed.)

            Note:  Table internal schema objects will not be accounted for.  These are tables that start with "sqlite_"
                   and are used for internal use to SQLite itself.  These have never known to produce any forensic
                   pertinent data. 

            """

            if isinstance(master_schema_entry, OrdinaryTableRow):

                if master_schema_entry.without_row_id:
                    log_message = f"A `without row_id` table was found: {master_schema_entry.table_name} and will not" \
                                  " have a signature generated for carving since it is not supported yet."
                    logger.info(log_message)
                    continue

                if master_schema_entry.internal_schema_object:
                    log_message = f"A `internal schema` table was found: {master_schema_entry.table_name} and will " \
                                  f"not have a signature generated for carving since it is not supported yet."
                    logger.info(log_message)
                    continue

                signatures[master_schema_entry.name] = Signature(version_history, master_schema_entry)

                printable_signature = signatures[master_schema_entry.name].stringify("\t", False, False, False)
                logger.debug(f"\nSignature:\n{printable_signature}")
                if print_signatures:
                    print(f"\nSignature:\n{printable_signature}")

    """

    Note:  Master schema entries (schema) are all pulled from the base version (the SQLite database file).  Currently,
           the master schema entries are taken from the base version.  Even though schema additions are handled in the
           WAL file for existing tables, tables added in the WAL have not been accounted for yet.

    """

    # Set the flag to determine if any of the export types were successful. Since the logic was changed from elif logic
    # to account for the simultaneous multiple export formats, it can't just cascade down.
    exported = False
    export_paths = []

    # Export to text
    if EXPORT_TYPES.TEXT in export_types:
        exported = True
        export_paths += print_text(output_directory, file_prefix, carve, carve_freelists, specified_tables_to_carve,
                                   version_history, signatures, logger)

    # Export to CSV
    if EXPORT_TYPES.CSV in export_types:
        exported = True
        export_paths += print_csv(output_directory, file_prefix, carve, carve_freelists,
                                  specified_tables_to_carve, version_history, signatures, logger)

    # Export to SQLite
    if EXPORT_TYPES.SQLITE in export_types:
        exported = True
        export_paths.append(print_sqlite(output_directory, file_prefix, carve, carve_freelists,
                                         specified_tables_to_carve, version_history, signatures, logger))

    # Export to XLSX
    if EXPORT_TYPES.XLSX in export_types:
        exported = True
        export_paths.append(print_xlsx(output_directory, file_prefix, carve, carve_freelists,
                                       specified_tables_to_carve, version_history, signatures, logger))

    # Export to CASE
    if EXPORT_TYPES.CASE in export_types:
        exported = True

        # Add the header and get the GUID for the tool for future linking
        tool_guid = case.generate_header()

        # Add the runtime arguments to the CASE output
        case.register_options(arguments)

        # Add the SQLite/DB file to the CASE output
        source_guids = [case.add_observable_file(normpath(sqlite_file_path), 'sqlite-file')]

        # Add the WAL and journal files to the output if they exist
        if wal_file_name:
            source_guids.append(case.add_observable_file(normpath(wal_file_name), 'wal-file'))
        if rollback_journal_file_name:
            source_guids.append(case.add_observable_file(normpath(rollback_journal_file_name), 'journal-file'))

        # Add the export artifacts and link them to the original source file
        case.add_export_artifacts(export_paths)

        # End the investigation output timer
        case.end_datetime = datetime.now()

        # Trigger the generation of the investigative action since the start and end time have now been set
        case.generate_investigation_action(source_guids, tool_guid)

        # Export the output to a JSON file
        case.export_case_file(path.join(arguments.directory, 'case.json'))

    # The export type was not found (this should not occur due to the checking of argparse)
    if not exported:
        raise SqliteError(f"Invalid option for export type: {(', '.join(export_types))}.")

    # Carve the rollback journal if found and carving is not specified
    if rollback_journal_file and not carve:
        logger.warning(f"Rollback journal file found: {rollback_journal_file}. Rollback journal file parsing is under "
                       f"development and currently only supports carving. Please rerun with the --carve option for this"
                       f" output.")

    # Carve the rollback journal if found and carving is specified
    if rollback_journal_file and carve:

        if not output_directory:

            logger.error(f"Rollback journal file found: {rollback_journal_file}. Rollback journal file carving is "
                         f"under development and currently only outputs to CSV. Due to this, the output directory "
                         f"needs to be specified. Please rerun with a output directory specified in order for this to "
                         f"complete.")

        else:

            logger.error(f"Carving rollback journal file: {rollback_journal_file}. Rollback journal file carving is "
                         f"under development and currently only outputs to CSV. Any export type specified will be "
                         f"overridden for this.")

            carve_rollback_journal(output_directory, rollback_journal_file, rollback_journal_file_name,
                                   specified_tables_to_carve, rollback_journal_exempted_tables,
                                   version_history, signatures, logger)

    logger.info(f"Finished in {round(time() - start_time, 2)} seconds.")


def print_text(output_directory, file_prefix, carve, carve_freelists, specified_tables_to_carve,
               version_history, signatures, logger):
    """
    Prints the text log to the output directory or stdout console as configured, and returns the path of any file(s)
    that are generated as a result of the export
    """
    export_paths = []

    if output_directory:

        file_postfix = ".txt"
        text_file_name = file_prefix + file_postfix

        # Export all index and table histories to a text file while supplying signature to carve with
        print(f"\nExporting history as text to {output_directory}{sep}{text_file_name}...")
        logger.debug(f"Exporting history as text to {output_directory}{sep}{text_file_name}.")

        with CommitTextExporter(output_directory, text_file_name) as commit_text_exporter:

            for master_schema_entry in version_history.versions[BASE_VERSION_NUMBER] \
                    .master_schema.master_schema_entries:

                # Only account for the specified tables
                if specified_tables_to_carve and master_schema_entry.name not in specified_tables_to_carve:
                    continue

                if master_schema_entry.row_type in [MASTER_SCHEMA_ROW_TYPE.INDEX, MASTER_SCHEMA_ROW_TYPE.TABLE]:

                    signature = None
                    if carve:
                        signature = signatures[master_schema_entry.name] if master_schema_entry.name in signatures \
                            else None

                        if not signature and master_schema_entry.row_type is MASTER_SCHEMA_ROW_TYPE.TABLE \
                                and not master_schema_entry.without_row_id \
                                and not master_schema_entry.internal_schema_object:
                            print(f"Unable to find signature for: {master_schema_entry.name}. This table will not be "
                                  f"carved.")
                            logger.error(f"Unable to find signature for: {master_schema_entry.name}. This table will "
                                         f"not be carved.")

                    if signature:
                        version_history_parser = VersionHistoryParser(version_history, master_schema_entry, None, None,
                                                                      signature, carve_freelists)
                    else:
                        version_history_parser = VersionHistoryParser(version_history, master_schema_entry)

                    page_type = version_history_parser.page_type
                    commit_text_exporter.write_header(master_schema_entry, page_type)

                    for commit in version_history_parser:
                        commit_text_exporter.write_commit(commit)

        # Append the output file to the exported file list
        logger.debug('Adding exported file to export_paths {}'.format(join(output_directory, text_file_name)))
        export_paths.append(join(output_directory, text_file_name))

    else:

        # Export all index and table histories to csv files while supplying signature to carve with
        logger.debug("Exporting history to console as text.")

        for master_schema_entry in version_history.versions[BASE_VERSION_NUMBER].master_schema.master_schema_entries:

            # Only account for the specified tables
            if specified_tables_to_carve and master_schema_entry.name not in specified_tables_to_carve:
                continue

            if master_schema_entry.row_type in [MASTER_SCHEMA_ROW_TYPE.INDEX, MASTER_SCHEMA_ROW_TYPE.TABLE]:

                signature = None
                if carve:
                    signature = signatures[master_schema_entry.name] if master_schema_entry.name in signatures else None

                    if not signature and master_schema_entry.row_type is MASTER_SCHEMA_ROW_TYPE.TABLE \
                            and not master_schema_entry.without_row_id \
                            and not master_schema_entry.internal_schema_object:
                        print(f"Unable to find signature for: {master_schema_entry.name}. This table will not be "
                              f"carved.")
                        logger.error(f"Unable to find signature for: {master_schema_entry.name}. This table will not "
                                     f"be carved.")

                if signature:
                    version_history_parser = VersionHistoryParser(version_history, master_schema_entry, None, None,
                                                                  signature, carve_freelists)
                else:
                    version_history_parser = VersionHistoryParser(version_history, master_schema_entry)

                page_type = version_history_parser.page_type
                CommitConsoleExporter.write_header(master_schema_entry, page_type)

                for commit in version_history_parser:
                    CommitConsoleExporter.write_commit(commit)

            print('-' * 15)

    return export_paths


def print_csv(output_directory, file_prefix, carve, carve_freelists, specified_tables_to_carve,
              version_history, signatures, logger):
    # Export all index and table histories to csv files while supplying signature to carve with
    logger.info(f"Exporting history to {output_directory} as CSV.")

    commit_csv_exporter = CommitCsvExporter(output_directory, file_prefix)

    for master_schema_entry in version_history.versions[BASE_VERSION_NUMBER].master_schema.master_schema_entries:

        # Only account for the specified tables
        if specified_tables_to_carve and master_schema_entry.name not in specified_tables_to_carve:
            continue

        if master_schema_entry.row_type in [MASTER_SCHEMA_ROW_TYPE.INDEX, MASTER_SCHEMA_ROW_TYPE.TABLE]:

            signature = None
            if carve:
                signature = signatures[master_schema_entry.name] if master_schema_entry.name in signatures else None

                if not signature and master_schema_entry.row_type is MASTER_SCHEMA_ROW_TYPE.TABLE \
                        and not master_schema_entry.without_row_id \
                        and not master_schema_entry.internal_schema_object:
                    logger.error(f"Unable to find signature for: {master_schema_entry.name}.  This table will not be "
                                 f"carved.")

            if signature:
                version_history_parser = VersionHistoryParser(version_history, master_schema_entry, None, None,
                                                              signature, carve_freelists)
            else:
                version_history_parser = VersionHistoryParser(version_history, master_schema_entry)

            for commit in version_history_parser:
                commit_csv_exporter.write_commit(master_schema_entry, commit)

    # Return the list of CSV filenames that were generated as part of the output
    return list(commit_csv_exporter.csv_file_names.values())


def print_sqlite(output_directory, file_prefix, carve, carve_freelists,
                 specified_tables_to_carve, version_history, signatures, logger):
    file_postfix = "-sqlite-dissect.db3"
    sqlite_file_name = file_prefix + file_postfix

    logger.debug("Exporting history as SQLite to {}{}{}.".format(output_directory, sep, sqlite_file_name))

    with CommitSqliteExporter(output_directory, sqlite_file_name) as commit_sqlite_exporter:

        for master_schema_entry in version_history.versions[BASE_VERSION_NUMBER].master_schema.master_schema_entries:

            # Only account for the specified tables
            if specified_tables_to_carve and master_schema_entry.name not in specified_tables_to_carve:
                continue

            if master_schema_entry.row_type in [MASTER_SCHEMA_ROW_TYPE.INDEX, MASTER_SCHEMA_ROW_TYPE.TABLE]:

                signature = None
                if carve:
                    signature = signatures[master_schema_entry.name] if master_schema_entry.name in signatures else None

                    if not signature and master_schema_entry.row_type is MASTER_SCHEMA_ROW_TYPE.TABLE \
                            and not master_schema_entry.without_row_id \
                            and not master_schema_entry.internal_schema_object:
                        logger.error("Unable to find signature for: {}.  This table will not be carved."
                                     .format(master_schema_entry.name))

                if signature:
                    version_history_parser = VersionHistoryParser(version_history, master_schema_entry, None, None,
                                                                  signature, carve_freelists)
                else:
                    version_history_parser = VersionHistoryParser(version_history, master_schema_entry)

                for commit in version_history_parser:
                    commit_sqlite_exporter.write_commit(master_schema_entry, commit)

    return join(output_directory, sqlite_file_name)


def print_xlsx(output_directory, file_prefix, carve, carve_freelists, specified_tables_to_carve,
               version_history, signatures, logger):
    file_postfix = ".xlsx"
    xlsx_file_name = file_prefix + file_postfix

    # Export all index and table histories to a xlsx workbook while supplying signature to carve with
    logger.debug("Exporting history as XLSX to {}{}{}.".format(output_directory, sep, xlsx_file_name))

    with CommitXlsxExporter(output_directory, xlsx_file_name) as commit_xlsx_exporter:

        for master_schema_entry in version_history.versions[BASE_VERSION_NUMBER].master_schema.master_schema_entries:

            # Only account for the specified tables
            if specified_tables_to_carve and master_schema_entry.name not in specified_tables_to_carve:
                continue

            if master_schema_entry.row_type in [MASTER_SCHEMA_ROW_TYPE.INDEX, MASTER_SCHEMA_ROW_TYPE.TABLE]:

                signature = None
                if carve:
                    signature = signatures[master_schema_entry.name] if master_schema_entry.name in signatures else None

                    if not signature and master_schema_entry.row_type is MASTER_SCHEMA_ROW_TYPE.TABLE \
                            and not master_schema_entry.without_row_id \
                            and not master_schema_entry.internal_schema_object:
                        logger.error("Unable to find signature for: {}.  This table will not be carved."
                                     .format(master_schema_entry.name))

                if signature:
                    version_history_parser = VersionHistoryParser(version_history, master_schema_entry, None, None,
                                                                  signature, carve_freelists)
                else:
                    version_history_parser = VersionHistoryParser(version_history, master_schema_entry)

                for commit in version_history_parser:
                    commit_xlsx_exporter.write_commit(master_schema_entry, commit)

    return join(output_directory, xlsx_file_name)


def carve_rollback_journal(output_directory, rollback_journal_file, rollback_journal_file_name,
                           specified_tables_to_carve, rollback_journal_exempted_tables,
                           version_history, signatures, logger):
    """

    Carve the Rollback Journal file (Under Development)

    Note: Since there is no normal parsing of the rollback journal file implemented yet, this is only done when
    carving is specified.  Also, since we are blindly carving each page in the rollback journal currently,
    we are not checking for pointer map pages, freelist pages, and so on.  Therefore, we do not care about the
    carve_freelist_pages option here.  The rollback journal file is being carved as it were all unallocated space.

    """

    csv_prefix_rollback_journal_file_name = basename(normpath(rollback_journal_file_name))
    logger.debug(f"Exporting rollback journal carvings as csv to output directory: {output_directory}.")

    commit_csv_exporter = CommitCsvExporter(output_directory, csv_prefix_rollback_journal_file_name)

    for master_schema_entry in version_history.versions[BASE_VERSION_NUMBER].master_schema.master_schema_entries:

        # Only account for the specified tables
        if specified_tables_to_carve and master_schema_entry.name not in specified_tables_to_carve:
            continue

        if master_schema_entry.name in rollback_journal_exempted_tables:
            logger.debug("Skipping exempted table: {} from rollback journal parsing.".format(master_schema_entry.name))
            continue

        """

        Only account for OrdinaryTableRow objects (not VirtualTableRow objects) that are not "without rowid" tables or
        internal schema objects. All signatures generated will not be outside this criteria either.

        """

        if isinstance(master_schema_entry, OrdinaryTableRow) and not master_schema_entry.without_row_id \
                and not master_schema_entry.internal_schema_object:

            signature = None
            if signatures and master_schema_entry.name in signatures:
                signature = signatures[master_schema_entry.name]

            # Make sure we found the error but don't error out if we don't.  Alert the user.
            if signature:
                # Carve the rollback journal with the signature
                carved_commits = RollBackJournalCarver.carve(rollback_journal_file,
                                                             version_history.versions[BASE_VERSION_NUMBER],
                                                             master_schema_entry, signature)

                for commit in carved_commits:
                    commit_csv_exporter.write_commit(master_schema_entry, commit)

            else:
                logger.error("Unable to find signature for: {}.  This table will not be carved from the "
                             "rollback journal.".format(master_schema_entry.name))


def cli():
    # Determine if a directory has been passed instead of a file, in which case, find all
    args = parse_args()
    if args.sqlite_path is not None:
        sqlite_files = get_sqlite_files(abspath(args.sqlite_path))
        # Ensure there is at least one SQLite file
        if len(sqlite_files) > 0:
            for sqlite_file in sqlite_files:
                # Call the main function
                main(args, sqlite_file, len(sqlite_files) > 1)
        else:
            raise SqliteError("No valid SQLite files were found in the provided path")
    else:
        raise SqliteError("No SQLite file or directory was passed")


if __name__ == "__main__":
    """
    Allows the script to be called without an explicit function call and calls the cli() function.
    """
    cli()
