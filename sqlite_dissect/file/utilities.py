from sqlite_dissect.file.wal.commit_record import WriteAheadLogCommitRecord

"""

utilities.py

This script holds utility functions for dealing with the version classes rather than more general utility methods.

This script holds the following function(s):
validate_page_version_history(version_history)

"""


def validate_page_version_history(version_history):
    for version_number, version in version_history.versions.iteritems():
        for page_number, page in version.pages.iteritems():
            if page.page_version_number != version.page_version_index[page.number]:
                return False
            if page.version_number != version.version_number:
                return False
            if isinstance(version, WriteAheadLogCommitRecord):
                if page_number in version.updated_page_numbers:
                    page_frame_index = version.page_frame_index
                    page_frame = page_frame_index[page.number]
                    actual_page_frame = version.frames[page.number].frame_number
                    if page_frame != actual_page_frame:
                        return False
    return True
