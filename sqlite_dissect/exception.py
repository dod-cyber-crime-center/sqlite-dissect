
"""

exception.py

This script holds the custom exceptions used in this library.

This script holds the following object(s):
SqliteError(Exception)
ParsingError(SqliteError)
HeaderParsingError(ParsingError)
MasterSchemaParsingError(ParsingError)
MasterSchemaRowParsingError(MasterSchemaParsingError)
PageParsingError(ParsingError)
BTreePageParsingError(PageParsingError)
CellParsingError(BTreePageParsingError)
RecordParsingError(CellParsingError)
VersionParsingError(ParsingError)
DatabaseParsingError(VersionParsingError)
WalParsingError(VersionParsingError)
WalFrameParsingError(WalParsingError)
WalCommitRecordParsingError(WalParsingError)
SignatureError(SqliteError)
CarvingError(SqliteError)
CellCarvingError(CarvingError)
InvalidVarIntError(CarvingError)
OutputError(SqliteError)
ExportError(SqliteError)

"""


class SqliteError(Exception):
    pass


class ParsingError(SqliteError):
    pass


class HeaderParsingError(ParsingError):
    pass


class MasterSchemaParsingError(ParsingError):
    pass


class MasterSchemaRowParsingError(MasterSchemaParsingError):
    pass


class PageParsingError(ParsingError):
    pass


class BTreePageParsingError(PageParsingError):
    pass


class CellParsingError(BTreePageParsingError):
    pass


class RecordParsingError(CellParsingError):
    pass


class VersionParsingError(ParsingError):
    pass


class DatabaseParsingError(VersionParsingError):
    pass


class WalParsingError(VersionParsingError):
    pass


class WalFrameParsingError(WalParsingError):
    pass


class WalCommitRecordParsingError(WalParsingError):
    pass


class SignatureError(SqliteError):
    pass


class CarvingError(SqliteError):
    pass


class CellCarvingError(CarvingError):
    pass


class InvalidVarIntError(CarvingError):
    pass


class OutputError(SqliteError):
    pass


class ExportError(SqliteError):
    pass
