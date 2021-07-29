from logging import getLogger
from re import compile
from warnings import warn
from sqlite_dissect.carving.carved_cell import CarvedBTreeCell
from sqlite_dissect.carving.utilities import generate_signature_regex
from sqlite_dissect.constants import BLOB_SIGNATURE_IDENTIFIER
from sqlite_dissect.constants import CELL_LOCATION
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import TEXT_SIGNATURE_IDENTIFIER
from sqlite_dissect.exception import CarvingError
from sqlite_dissect.exception import CellCarvingError

"""

carver.py

This script holds carver objects for identifying and parsing out cells from unallocated and
freeblock space in SQLite b-tree pages.

This script holds the following object(s):
SignatureCarver(Carver)

"""


class SignatureCarver(object):

    @staticmethod
    def carve_freeblocks(version, source, freeblocks, signature):

        """

        This function will carve the freeblocks list with the signature specified.

        Note: The signature that will be used from the signature object will be the simplified signature unless
              one does not exist (in the case where one was generated with no row entries), in which case the
              simplified schema signature will be used.

        Note: The serial type definition nomenclature does not include the serial type header size field in reference
              to the offsets and may also not include the first (or first byte of a multi-byte varint) serial type and
              therefor dubbed "definition" instead of header signifying only a portion of the header.

        :param version:
        :param source:
        :param freeblocks:
        :param signature:

        :return:

        """

        logger = getLogger(LOGGER_NAME)

        number_of_columns = signature.number_of_columns

        simplified_signature = signature.simplified_signature

        if not simplified_signature:
            simplified_signature = signature.recommended_schema_signature
            logger.debug("Using recommended schema signature: {}.".format(simplified_signature))
        else:
            logger.debug("Using simplified signature: {}.".format(simplified_signature))

        if not simplified_signature:
            log_message = "No signature was found."
            logger.error(log_message)
            raise CarvingError(log_message)

        """

        Since we are carving freeblocks here, we will remove the first column serial type.  This is due to the fact
        that the freeblock header overwrites the first four bytes of the cell which usually overwrites the first
        serial type in the header of the record since that is the fourth byte (assuming payload, row id, and header
        length (where applicable) are all less than 1 varint).

        """

        first_column_serial_types = simplified_signature[0]

        if BLOB_SIGNATURE_IDENTIFIER in first_column_serial_types or TEXT_SIGNATURE_IDENTIFIER in \
                first_column_serial_types:
            log_message = "A variable length serial type was found in the first column serial types: {} while" \
                          "carving freeblocks with signatures: {}.  Signatures starting with variable length serial " \
                          "types are not fully implemented and may result in carving false positives."
            log_message = log_message.format(first_column_serial_types, simplified_signature)
            logger.warn(log_message)
            warn(log_message, RuntimeWarning)

        # Retrieve and compile the serial type definition signature pattern
        serial_type_definition_signature_pattern = compile(generate_signature_regex(simplified_signature, True))

        # Initialize the carved cells
        carved_cells = []

        # Iterate through the freeblocks
        for freeblock in freeblocks:

            # Get the content for the current freeblock
            freeblock_content = freeblock.content

            # Initialize the list for the serial type definition match objects
            serial_type_definition_match_objects = []

            # Find all matches for the serial type definition signature pattern
            for serial_type_definition_match in serial_type_definition_signature_pattern.finditer(freeblock_content):
                serial_type_definition_match_objects.append(serial_type_definition_match)

            """

            In order to carve the freeblocks we have to start from the ending match and move backwards through the
            matches in the freeblock.  This is due to the fact that when a freeblock is made, it can be reallocated,
            and then have the entry deleted again in it expanding it back to the original size it previously was.  When
            a freeblock is reallocated it counts the space it needs from the end of the freeblock rather than from
            the beginning.  This means that the ending portion (usually the data) of the previous freeblock that was
            in the spot will be overwritten.  Therefore, there is a good chance we should be able to parse out the last
            match successfully, but will end up have truncated carvings "beneath" the last one.

            As an example freeblocks are overwritten in the following pattern:
                        [Third Freeblock Entry .............]
                    [Second Freeblock Entry ................]
            [First Freeblock Entry .........................]

            This can also be in the following pattern though:
                        [Allocated Cell Entry ..............]
                    [Second Freeblock Entry ................]
            [First Freeblock Entry .........................]

            In the above example we have the possibility of losing all of the data and being unable to parse anything
            but the header of the previous freeblocks.

            """

            """

            The cutoff offset will be initialized to the length of the freeblock content and then be updated for
            "beneath" freeblock entries to be the starting offset of the previous entry.  There is some variation on
            if this is the actual cutoff or not but will always be after the actual cutoff when done this way.
            It is just important to keep in mind that the previous freeblocks may actually be cutoff before this offset
            and the "above" freeblocks may go back that length for things like payload size, row id, serial type header
            length and the first serial type depending on the use case.

            """

            cutoff_offset = len(freeblock_content)

            page_offset = version.get_page_offset(freeblock.page_number)

            # Iterate through the serial type definition matches in reverse
            for serial_type_definition_match in reversed(serial_type_definition_match_objects):

                """

                For the serial type definition match objects returned from the iterator above, the match object has a
                start and a end function to get the beginning offset and ending offset.  This is done by calling
                start(0) or end (0) with 0 being the group number.  The ending offset is exclusive
                ie. [start(0):end(0)).

                """

                serial_type_definition_start_offset = serial_type_definition_match.start(0)
                serial_type_definition_end_offset = serial_type_definition_match.end(0)
                file_offset = page_offset + freeblock.start_offset + serial_type_definition_start_offset

                try:

                    # Create and append the carved b-tree cell to the carved cells list
                    carved_cells.append(CarvedBTreeCell(version, file_offset, source, freeblock.page_number,
                                                        CELL_LOCATION.FREEBLOCK,
                                                        freeblock.index, freeblock_content,
                                                        serial_type_definition_start_offset,
                                                        serial_type_definition_end_offset, cutoff_offset,
                                                        number_of_columns, signature,
                                                        first_column_serial_types, freeblock.byte_size))

                    # Update the cutoff offset
                    cutoff_offset = serial_type_definition_start_offset

                except (CellCarvingError, ValueError):
                    log_message = "Carved b-tree cell creation failed at file offset: {} page number: {} " \
                                  "cell source: {} in location: {} with partial serial type definition " \
                                  "start offset: {} and partial serial type definition end offset: {} with " \
                                  "cutoff offset of: {} number of columns: {} for master schema " \
                                  "entry with name: {} and table name: {}."
                    log_message = log_message.format(file_offset, freeblock.page_number, source,
                                                     CELL_LOCATION.UNALLOCATED_SPACE,
                                                     serial_type_definition_start_offset,
                                                     serial_type_definition_end_offset, cutoff_offset,
                                                     number_of_columns, signature.name, signature.table_name)
                    logger.warn(log_message)
                    warn(log_message, RuntimeWarning)

        # Return the cells carved from the freeblocks
        return carved_cells

    @staticmethod
    def carve_unallocated_space(version, source, page_number, unallocated_space_start_offset,
                                unallocated_space, signature, page_offset=None):

        """

        This function will carve the unallocated space with the signature specified.

        Note: The signature that will be used from the signature object will be the simplified signature unless
              one does not exist (in the case where one was generated with no row entries), in which case the
              simplified schema signature will be used.

        Note: The serial type definition nomenclature does not include the serial type header size field in reference
              to the offsets and may also not include the first (or first byte of a multi-byte varint) serial type and
              therefor dubbed "definition" instead of header signifying only a portion of the header.

        :param version:
        :param source:
        :param page_number:
        :param unallocated_space_start_offset:
        :param unallocated_space:
        :param signature:
        :param page_offset: Page offset if needed to be specified.  Currently only used for proof of concept
                            journal page parsing.

        :return:

        """

        logger = getLogger(LOGGER_NAME)

        number_of_columns = signature.number_of_columns

        simplified_signature = signature.simplified_signature

        if not simplified_signature:
            simplified_signature = signature.recommended_schema_signature
            logger.debug("Using recommended schema signature: {}.".format(simplified_signature))
        else:
            logger.debug("Using simplified signature: {}.".format(simplified_signature))

        if not simplified_signature:
            log_message = "No signature was found."
            logger.error(log_message)
            raise CarvingError(log_message)

        # Retrieve and compile the serial type definition signature pattern
        serial_type_definition_signature_pattern = compile(generate_signature_regex(simplified_signature))

        """

        In reference for supporting freeblocks and additional use cases in unallocated space:

        Currently, unallocated space is carved using a full signature (not removing the first serial type) in order
        to detect deleted entries.  This can result in the following two use cases in reference to deleted entries
        in the unallocated space:
        1.) Cell entries that were deleted or left over from a previous page being reused that ended up in the
            unallocated space where the serial type header (excepting possibly the header size) of the payload
            is in tact.  Due to the way cells are inserted from the back of the page moving forward it is very
            likely to have the beginning of the cell as well (but not a certainty).
        2.) Freeblocks that had either a payload, row id, or serial type header size that one or more of which were
            either 2 byte or greater varints.  This would push the serial type header (excepting possibly the header
            size) into the main body of the freeblock.  This is due to the fact that the freeblock overwrites the first
            4 bytes of the entry with the next freeblock offset and freeblock size.  A freeblock needs at least 4 bytes
            to exist, and if not, it is a fragment.  Keep in mind this is also assuming a b-tree table leaf page and
            may not be the case for b-tree index pages or b-tree table interiors.

            In comparison to the not detected use case below, it is important to note that the first serial type may
            also be a varint of length greater than 2 bytes and therefore still detected where The #1 use case below
            is true but would incorrectly determine the size of the varint causing issues parsing the body of the cell.
            Additional research and handling of this use case is needed.

        The use of a "full" signature will not detect:
        1.) Freeblocks that have a payload, row id, and serial type header size of 1 varint will end up having the first
            serial type overwritten (excepting the use case defined in #2 above) which will result in the entries
            not being carved unless checking for the signature without the first serial type, like freeblocks are done.

            There are a few ways to do this (very similar to the freeblock carving code above) and needs to
            be implemented.

        Discussion:  There are a few ways to determine freeblocks.  One way is to calculate the size of the serial type
                     definition plus 1 byte for the header (depending on size) and compare that to the previous byte to
                     see if it matches.  If it does, the full header should be in tact and the body content can be
                     calculated from the serial type definition.  (The body content may still be able to be calculated
                     from the serial type definition without finding the serial type header length assuming the rest of
                     the serial types are all existent (the first serial type or portion of first multi-byte varint
                     serial type is not missing).  Once the body content and header content are calculated, moving
                     backwards the bytes can be checked for the size of the freeblock + 4 adding on one byte for each
                     byte gone back that does not match the size (this is to account for larger than 1 byte varints for
                     payload or row id).  If this is within the acceptable range of the varint sizes and matches the
                     size, there is a good chance this is a freeblock.

                     Pseudocode:

                     serial_type_header_size =
                            ord(unallocated_space[serial_type_definition_start_offset - 1:
                                                  serial_type_definition_start_offset])

                     if serial_type_header_size ==
                                            serial_type_definition_end_offset - serial_type_definition_start_offset + 1
                        This is the serial type header size (1 is added for the one byte serial type header byte size).
                     else:
                        This is not the serial type header size or the first serial type may be a multi-byte varint
                        use case which would then cause this process to move back one byte and repeat or the serial
                        type header size may be a multi-byte varint.

                        However the third use case below should be predetermined in the above serial_type_header_size
                        setting statement based on the size between the serial_type_definition_end_offset and
                        serial_type_definition_start_offset.

                     After the above:

                     Given additional_serial_type_header_bytes is the amount of extra bytes for the header calculated
                     above and header_start_offset refers to the location the full header starts at:
                     calculated_payload_length = additional_serial_type_header_bytes +
                                                 serial_type_definition_end_offset -
                                                 serial_type_definition_start_offset + body_content_size + 4
                     if calculated_payload_length ==
                                    unpack(b">H", unallocated_space[header_start_offset - 2:header_start_offset])[0]:
                        There is a freeblock possibility but may also be a payload to a b-tree index cell.
                     else:
                        This may be a table leaf cell where this first number would be the row id, in which we should
                        reverse parse out the varint and then check the next index for the size (excepting adding in the
                        size of the row id since the payload size is only the actual payload following the row id).

                    A similar process could be used for parsing out cells that are not freeblocks in order to determine
                    things such as payload size, row id, serial type header length, or missing (or partially missing
                    portion of a multi-byte varint) first serial type in actual cells.  This will be left up to the
                    CarvedBTreeCell class to do and the above documentation may end up applying more to that class
                    then here.

        Note:  Overflow still needs to be addressed.

        Note:  The above use cases have been determined from investigation into how SQLite stores data and may not be
               a complete list.

        """

        # Initialize the list for the serial type definition match objects
        serial_type_definition_match_objects = []

        # Find all matches for the serial type definition signature pattern
        for serial_type_definition_match in serial_type_definition_signature_pattern.finditer(unallocated_space):
            serial_type_definition_match_objects.append(serial_type_definition_match)

        # Initialize the carved cells
        carved_cells = []

        """

        Like above, in the freeblock carving code, we find all of the matches for the signature and then work in reverse
        through the unallocated space.  The idea here is very similar to the freeblock carving (see the documentation
        above) since cells are added from the unallocated space at the end of the page moving back towards the front
        of the page much like how cells are added back into freeblocks from the end if there is enough space.

        """

        """

        The cutoff offset will be initialized to the length of the unallocated space and then be updated for
        entries that may have been overwritten previously by the entries at the end of the unallocated space.
        There is some variation on if this is the actual cutoff or not but will always be after the actual cutoff
        when done this way.  It is just important to keep in mind that the previous entries (including possibly
        freeblocks) may actually be cutoff before this offset and the entries overwritten on top of previous entries
        may go back that length for things like payload size, row id, serial type header length and the first serial
        type depending on the use case.

        """

        cutoff_offset = len(unallocated_space)

        # Retrieve the page offset if it was not set through the constructor (should only be set for
        # proof of concept journal file parsing).
        if page_offset is None:
            page_offset = version.get_page_offset(page_number)

        # Iterate through the serial type definition matches in reverse
        for serial_type_definition_match in reversed(serial_type_definition_match_objects):

            """

            For the serial type definition match objects returned from the iterator above, the match object has a
            start and a end function to get the beginning offset and ending offset.  This is done by calling
            start(0) or end (0) with 0 being the group number.  The ending offset is exclusive ie. [start(0):end(0)).

            """

            serial_type_definition_start_offset = serial_type_definition_match.start(0)
            serial_type_definition_end_offset = serial_type_definition_match.end(0)
            file_offset = page_offset + unallocated_space_start_offset + serial_type_definition_start_offset

            try:

                # Create and append the carved b-tree cell to the carved cells list
                carved_cells.append(CarvedBTreeCell(version, file_offset, source, page_number,
                                                    CELL_LOCATION.UNALLOCATED_SPACE, 0, unallocated_space,
                                                    serial_type_definition_start_offset,
                                                    serial_type_definition_end_offset, cutoff_offset,
                                                    number_of_columns, signature))

                # Update the cutoff offset
                cutoff_offset = serial_type_definition_start_offset

            except (CellCarvingError, ValueError):
                log_message = "Carved b-tree cell creation failed at file offset: {} page number: {} " \
                              "cell source: {} in location: {} with partial serial type definition " \
                              "start offset: {} and partial serial type definition end offset: {} with " \
                              "cutoff offset of: {} number of columns: {} for master schema " \
                              "entry with name: {} and table name: {}."
                log_message = log_message.format(file_offset, page_number, source,
                                                 CELL_LOCATION.UNALLOCATED_SPACE,
                                                 serial_type_definition_start_offset,
                                                 serial_type_definition_end_offset, cutoff_offset,
                                                 number_of_columns, signature.name, signature.table_name)
                logger.warn(log_message)
                warn(log_message, RuntimeWarning)

        """

        At this point we have carved all the "full signatures" in reference to the full serial type definition in
        the cell headers that we found.  However, although the above may be freeblocks in the unallocated space (in
        the use case where the combination of the payload, row id, and/or payload header varint equate out to 4 or
        more bytes), the use case still remains where all 3 are 1 byte as well as the first serial type.  In this case
        we would only have the 2nd through Nth serial types like the above code in carve freeblocks.  Therefore, we
        recompute the signature removing the first serial type, recheck for patterns and if they do not match the
        patterns above, add them as well.


        Note:  If this matches, it does not mean this is necessarily a freeblock since it could have just have been a
               cell removed and then overwritten partially by another cell.  Use cases like these should be addressed
               in the carved cell classes.

        """

        # Reset the signature pattern removing the first serial type and compile
        serial_type_definition_signature_pattern = compile(generate_signature_regex(simplified_signature, True))

        # Initialize the list for the partial serial type definition match objects
        partial_serial_type_definition_match_objects = []

        # Find all matches for the partial serial type definition signature pattern
        for serial_type_definition_match in serial_type_definition_signature_pattern.finditer(unallocated_space):
            partial_serial_type_definition_match_objects.append(serial_type_definition_match)

        """

        The partial serial type definition match objects should now be a superset of the serial type definition match
        objects above.  We now go through these match objects and remove any of the data segments found above by
        comparing the indices.

        Note:  This is done after instead of before the full serial type signature matching since it is more conclusive
               to carve the whole cells rather than the ones without the full serial type header.

        Note:  The indices should be updated with the correct cutoff offset and beginning offset where found in the
               carved cells from the match objects.  Currently, these indices only reflect the serial type definition
               header.  This will further improve the validity of the result set.  This will be done once the carved
               cell class and use cases are fully handled.

        """

        # Create a list of all ending indices for the serial type definition match objects and sort by beginning index
        serial_type_definition_match_objects_indices = sorted([(match_object.start(0), match_object.end(0))
                                                              for match_object in serial_type_definition_match_objects],
                                                              key=lambda x: x[0])

        unallocated_space_length = len(unallocated_space)
        serial_type_definition_match_objects_indices_length = len(serial_type_definition_match_objects_indices)
        uncarved_unallocated_space_indices = []

        # If there were no serial type definition matches, we set the whole unallocated space to be checked
        if not serial_type_definition_match_objects_indices:
            uncarved_unallocated_space_indices.append((0, unallocated_space_length))

        else:
            last_offset = None
            for index, match_object_index in enumerate(serial_type_definition_match_objects_indices):

                if index == 0 and index != len(serial_type_definition_match_objects_indices) - 1:

                    """

                    Check if we are at the first index and if there are additional indexes in the match object.  If
                    this is the case, add the section of data from the beginning of the unallocated data to the
                    beginning of this index.  This is only done if data is found.  If there is no data (ie. the first
                    index of the first match object is the first index of the unallocated data), then we do not set
                    the new index on this first iteration.

                    """

                    if match_object_index[0] != 0:
                        uncarved_unallocated_space_indices.append((0, match_object_index[0]))
                        last_offset = match_object_index[1]

                elif index == 0 and index == serial_type_definition_match_objects_indices_length - 1:

                    """

                    Check if we are at the first index and if there are no additional indexes in the match object.  If
                    this is the case, we add an index from the beginning of the unallocated data to the first index of
                    the first (and only) match index.  If there is data between the ending index of the match we are
                    currently looking at and the end of the unallocated space, we add an index from the ending match
                    index to the ending of the unallocated data.

                    """

                    uncarved_unallocated_space_indices.append((0, match_object_index[0]))
                    if match_object_index[1] != len(unallocated_space):
                        uncarved_unallocated_space_indices.append((match_object_index[1], unallocated_space_length))
                        last_offset = match_object_index[1]

                elif index != 0 and index != serial_type_definition_match_objects_indices_length - 1:

                    """

                    If we are not on the first index and there are more indexes to come, we just add the data portion
                    between the ending offset of the last match offset and the beginning index of this first match
                    offset.

                    """

                    uncarved_unallocated_space_indices.append((last_offset, match_object_index[0]))
                    last_offset = match_object_index[1]

                elif index != 0 and index == serial_type_definition_match_objects_indices_length - 1:

                    """

                    If we are not on the first index and this is the last index of the previous match objects, we then
                    add the index of the last entry and the first index of this match object.  Then, if there is data
                    left in the unallocated space between the ending index of this match object and the end of the
                    unallocated space, we add the last entry between these indices.

                    """

                    uncarved_unallocated_space_indices.append((last_offset, match_object_index[0]))
                    if match_object_index[1] != len(unallocated_space):
                        uncarved_unallocated_space_indices.append((match_object_index[1], unallocated_space_length))
                else:

                    log_message = "Found invalid use case while carving unallocated space for page number: {} " \
                                  "starting from the unallocated space start offset: {} with signature: {}."
                    log_message = log_message.format(page_number, unallocated_space_start_offset, signature.name)
                    logger.error(log_message)
                    raise CarvingError(log_message)

        """

        Iterate through the uncarved portions of the unallocated space and update the cutoff offset to the be the
        min index of the previous partial cutoff offset and the current uncarved allocated space index ending offset.

        """

        partial_cutoff_offset = len(unallocated_space)
        for partial_serial_type_definition_match in reversed(partial_serial_type_definition_match_objects):
            for uncarved_allocated_space_index in reversed(uncarved_unallocated_space_indices):

                cutoff_offset = min(uncarved_allocated_space_index[1], partial_cutoff_offset)

                partial_serial_type_definition_start_offset = partial_serial_type_definition_match.start(0)
                partial_serial_type_definition_end_offset = partial_serial_type_definition_match.end(0)

                if partial_serial_type_definition_start_offset >= uncarved_allocated_space_index[0] and \
                   partial_serial_type_definition_end_offset <= uncarved_allocated_space_index[1]:

                        relative_offset = unallocated_space_start_offset + partial_serial_type_definition_start_offset
                        file_offset = page_offset + relative_offset
                        first_column_serial_types = simplified_signature[0]

                        try:

                            # Create and append the carved b-tree cell to the carved cells list
                            carved_cells.append(CarvedBTreeCell(version, file_offset, source, page_number,
                                                                CELL_LOCATION.UNALLOCATED_SPACE,
                                                                0, unallocated_space,
                                                                partial_serial_type_definition_start_offset,
                                                                partial_serial_type_definition_end_offset,
                                                                cutoff_offset, number_of_columns, signature,
                                                                first_column_serial_types))

                            # Update the partial cutoff offset
                            partial_cutoff_offset = partial_serial_type_definition_start_offset

                        except (CellCarvingError, ValueError):
                            log_message = "Carved b-tree cell creation failed at file offset: {} page number: {} " \
                                          "cell source: {} in location: {} with partial serial type definition " \
                                          "start offset: {} and partial serial type definition end offset: {} with " \
                                          "partial cutoff offset of: {} number of columns: {} for master schema " \
                                          "entry with name: {} and table name: {}."
                            log_message = log_message.format(file_offset, page_number, source,
                                                             CELL_LOCATION.UNALLOCATED_SPACE,
                                                             partial_serial_type_definition_start_offset,
                                                             partial_serial_type_definition_end_offset,
                                                             partial_cutoff_offset, number_of_columns, signature.name,
                                                             signature.table_name)
                            logger.warn(log_message)
                            warn(log_message, RuntimeWarning)

        # Return the cells carved from the freeblocks
        return carved_cells
