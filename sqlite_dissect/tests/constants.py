import os

SUCCESS = "Success"
IO_ERROR = "IO Error"
VALUE_ERROR = "Value Error"
NOT_IMPLEMENTED_ERROR = "Not Implemented Error"
HEADER_ERROR = "Header Parsing Error"
TYPE_ERROR = "Type Error"
RUNTIME_WARNING = "Runtime Warning"
EOF_ERROR = "End of File Error"
RECORD_ERROR = "Record Parsing Error"
VERSION_ERROR = "Version Parsing Error"
KEY_ERROR = "Key Error"
DB_FILES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_files")
LOG_FILES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log_files")
