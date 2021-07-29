from logging import getLogger
from re import sub
from warnings import warn
from sqlite_dissect.constants import FILE_TYPE
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import WAL_FRAME_HEADER_LENGTH
from sqlite_dissect.constants import WAL_HEADER_LENGTH
from sqlite_dissect.exception import WalParsingError
from sqlite_dissect.file.file_handle import FileHandle
from sqlite_dissect.file.wal.frame import WriteAheadLogFrame

"""

frame.py

This script holds the WAL objects used for parsing the WAL file.

This script holds the following object(s):
WriteAheadLog(object)

"""


class WriteAheadLog(object):

    def __init__(self, file_identifier, store_in_memory=False, file_size=None, strict_format_checking=True):

        """

        Constructor.

        :param file_identifier: str or file  The full file path to the file to be opened or the file object.
        :param store_in_memory: boolean  Tells this class to store it's particular version information in memory or not.
        :param file_size: int  Optional parameter to supply the file size.
        :param strict_format_checking: boolean  Specifies if the application should exit if structural validations fail.

        """

        self.file_handle = FileHandle(FILE_TYPE.WAL, file_identifier, file_size=file_size)
        self.store_in_memory = store_in_memory
        self.strict_format_checking = strict_format_checking

        logger = getLogger(LOGGER_NAME)

        frame_size = (WAL_FRAME_HEADER_LENGTH + self.file_handle.header.page_size)

        self.number_of_frames = (self.file_handle.file_size - WAL_HEADER_LENGTH) / frame_size

        valid_frame_array = []
        invalid_frame_array = []
        commit_record_number = 1

        """

        Since we have the possibility of WAL files executing checkpoints and overwriting themselves, we can have
        invalid frames trailing the valid frames.  The calculations above will always prove true since the frames are
        always the same size they will always fully overwrite.  Therefore, we should never come across a situation
        where a WAL file has partially overwritten WAL frames in it (assuming the file is not damaged itself).

        In order to keep track of the invalid frames, we index the starting and ending frame indices that we find those
        frames that correlate to a particular salt 1 value together.  Salt 1 values are incremented on checkpoint
        operations.  Therefore we can determine the order of how the invalid frames were stored into the file by
        looking at the checkpoint number and correlating the offset of the salt 1 value from the salt 1 value in
        the WAL file header.

        When we find invalid frames, we will set the commit record number to None for now until further implemented.

        Below we initialize dictionary of salt 1 value to a tuple where the first and second values apply to the first
        invalid frame index found and last invalid frame index found for that salt 1 value.  Due to the way WAL files
        overwrite and commit we should always have at least one frame in this use case at if it is only one frame, or
        the last frame found, should always be a commit frame (ie. where the database page size after commit is set).

        Also, if there are any entries in the invalid frame indices when a valid frame is found, an exception is raised
        since this should never occur.

        """

        # Initialize the dictionary
        self.invalid_frame_indices = {}

        for frame_index in range(self.number_of_frames):

            frame = WriteAheadLogFrame(self.file_handle, frame_index, commit_record_number)

            # Check if the salt 1 values were different (invalid frame)
            if frame.header.salt_1 != self.file_handle.header.salt_1:

                log_message = "Frame index: {} after commit record number: {} has salt 1 of {} when expected to " \
                              "be: {} and is an invalid frame."
                log_message = log_message.format(frame_index, commit_record_number - 1, frame.header.salt_1,
                                                 self.file_handle.header.salt_1)
                logger.debug(log_message)

                # Check if this salt value was already put into the invalid frame indices dictionary
                if frame.header.salt_1 in self.invalid_frame_indices:

                    # Get the previous indices
                    indices = self.invalid_frame_indices[frame.header.salt_1]

                    # Check to make sure this frame index is the next one in the array
                    if indices[1] + 1 != frame_index:
                        log_message = "Frame index: {} with salt 1 of {} when expected to be: {} after commit " \
                                      "record number: {} has a different frame index than the expected: {}."
                        log_message = log_message.format(frame_index, frame.header.salt_1,
                                                         self.file_handle.header.salt_1, commit_record_number - 1,
                                                         indices[1] + 1)
                        logger.error(log_message)
                        raise WalParsingError(log_message)

                    # Add the updated indices for the WAL value into the invalid frame indices dictionary
                    self.invalid_frame_indices[frame.header.salt_1] = (indices[0], frame_index)

                # The salt value was not already put into the invalid frame indices dictionary
                else:

                    # Add the indices for the salt value into the invalid frame indices dictionary
                    self.invalid_frame_indices[frame.header.salt_1] = (frame_index, frame_index)

                # Update the commit record number to None (see above documentation and script header documentation)
                frame.commit_record_number = None

                # Append the frame to the invalid frame array
                invalid_frame_array.append(frame)

            # Check if the salt 2 values were different if the salt 1 values were the same (error)
            elif frame.header.salt_2 != self.file_handle.header.salt_2:

                log_message = "Frame index: {} after commit record number: {} has salt 2 of {} when expected to " \
                              "be: {} where the salt 1 values matched."
                log_message = log_message.format(frame_index, commit_record_number - 1, frame.header.salt_1,
                                                 self.file_handle.header.salt_1)
                logger.error(log_message)
                raise WalParsingError(log_message)

            # The frame is a valid frame
            else:

                # Make sure there are no entries in the invalid frame indices or else there was an error
                if self.invalid_frame_indices:
                    log_message = "Frame index: {} in commit record number: {} follows invalid frames."
                    log_message = log_message.format(frame_index, commit_record_number)
                    logger.error(log_message)
                    raise WalParsingError(log_message)

                # Append the frame to the valid frame array and increment the commit record number for a commit frame
                valid_frame_array.append(frame)
                if frame.commit_frame:
                    commit_record_number += 1

        self.frames = dict(map(lambda x: [x.frame_index, x], valid_frame_array))
        self.invalid_frames = dict(map(lambda x: [x.frame_index, x], invalid_frame_array))

        # Check if we had invalid frames
        if self.invalid_frames:

            # Print debug log messages on the WAL frame details
            log_message = "The number of frames found in the wal file are: {} with {} valid frames between frame" \
                          "indices {} and {} and {} invalid frames between frame indices {} and {}"
            log_message = log_message.format(self.number_of_frames, len(self.frames), min(self.frames.keys()),
                                             max(self.frames.keys()), len(self.invalid_frames),
                                             min(self.invalid_frames.keys()), max(self.invalid_frames.keys()))
            logger.debug(log_message)

            log_message = "The invalid frame indices pertaining to salt 1 values are: {}."
            log_message = log_message.format(self.invalid_frame_indices)
            logger.debug(log_message)

            """

            Below we output a warning and a log message warning that implementation for invalid frames is not 
            handled or parsed yet.

            """

            log_message = "The wal file contains {} invalid frames.  Invalid frames are currently skipped and not " \
                          "implemented which may cause loss in possible carved data at this time until implemented."
            log_message = log_message.format(len(self.invalid_frames))
            logger.warn(log_message)
            warn(log_message, RuntimeWarning)

        self.last_frame_commit_record = None
        last_wal_frame_commit_record_index = max(self.frames.keys())
        while last_wal_frame_commit_record_index >= 0:

            """

            Starting from the end of the file and working backwards, we find the last commit record in the file
            to determine at which point the data was committed to the database file.  Soon as we find that frame,
            we break from the while loop.

            """

            if self.frames[last_wal_frame_commit_record_index].header.page_size_after_commit != 0:
                self.last_frame_commit_record = self.frames[last_wal_frame_commit_record_index]
                break
            else:
                last_wal_frame_commit_record_index -= 1

        if last_wal_frame_commit_record_index != len(self.frames) - 1:

            """

            If the last WAL frame commit record index does not equal the number of frames, that means that there was
            at least one entry in the WAL file beyond the last committed record.  This use case has not been discovered
            yet and a NotImplementedError will be raised here until the use case is handled.

            """

            log_message = "The last wal frame commit record index: {} was not the last committed frame of in {} frames."
            log_message = log_message.format(last_wal_frame_commit_record_index, len(self.frames))
            logger.error(log_message)
            raise NotImplementedError(log_message)

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding="", print_frames=True):
        string = padding + "File Handle:\n{}"
        string = string.format(self.file_handle.stringify(padding + "\t"))
        string += "\n" \
                  + padding + "Number of Frames: {}\n" \
                  + padding + "Number of Valid Frames: {}\n" \
                  + padding + "Number of Invalid Frames: {}\n" \
                  + padding + "Invalid Frames Indices: {}\n" \
                  + padding + "Last Frame Commit Record Number: {}"
        string = string.format(self.number_of_frames,
                               len(self.frames),
                               len(self.invalid_frames),
                               self.invalid_frame_indices,
                               self.last_frame_commit_record.frame_index + 1)
        if print_frames:
            for frame in self.frames.itervalues():
                string += "\n" + padding + "Frame:\n{}".format(frame.stringify(padding + "\t"))
        if print_frames and self.invalid_frames:
            for invalid_frame in self.invalid_frames.itervalues():
                string += "\n" + padding + "Invalid Frame:\n{}".format(invalid_frame.stringify(padding + "\t"))
        return string
