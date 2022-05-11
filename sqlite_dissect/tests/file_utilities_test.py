import pytest
from os.path import abspath, join, dirname, splitext
import random
from sqlite_dissect.file.utilities import validate_page_version_history
from sqlite_dissect.file.database.database import Database
from sqlite_dissect.tests.constants import DB_FILES
from sqlite_dissect.version_history import VersionHistory, WriteAheadLogCommitRecord
from sqlite_dissect.file.wal.wal import WriteAheadLog

validate_page_version_history_params = [
    (0, True),
    (1, False),
    (2, False),
    (3, False)
]


@pytest.mark.parametrize('change, expected_result', validate_page_version_history_params)
def test_validate_page_version_history(change, expected_result):
    # uses the version_history_test.sqlite file as a template; assumed to be valid at start
    db_filepath = abspath(join(DB_FILES, 'version_history_test.sqlite'))
    wal_filepath = splitext(db_filepath)[0] + '.sqlite-wal'

    db = Database(db_filepath)
    wal = WriteAheadLog(wal_filepath)
    version_history = VersionHistory(db, wal)

    # no change to version_history; should evaluate to True if original was valid
    if change == 0:
        assert validate_page_version_history(version_history) == expected_result

    # modify single values; should evaluate to False (invalid history)
    else:
        modified = False

        for version_number, version in version_history.versions.iteritems():
            for page_number, page in version.pages.iteritems():
                # modifies first page version number
                if change == 1:
                    page.page_version_number += 1
                    modified = True
                    break

                # modifies first version number
                elif change == 2:
                    page.version_number += 1
                    modified = True
                    break

                # modifies first page frame
                elif change == 3 and isinstance(version, WriteAheadLogCommitRecord) \
                        and page_number in version.updated_page_numbers:
                    choices = [number for number in version.page_frame_index.keys() if number != page_number]
                    version.page_frame_index[page_number] = version.page_frame_index[random.choice(choices)]
                    modified = True
                    break

            if modified:
                break

        assert validate_page_version_history(version_history) == expected_result

