from typing import List

from filelineindex.core.abstract import LineBatchedStorage, LineIndex


class BatchKeyData:
    """Data structure representing the key batch information."""

    def __init__(self, last_line: str, batch_start_lines: List[str]):
        """
        Initialize IndexData with the last line and a list of batch starting lines.

        :param last_line: The last line in the indexed set.
        :param batch_start_lines: List of starting lines for each batch in the index.
        """
        self.last_line: str = last_line
        self.batch_start_lines: List[str] = batch_start_lines


class LineBatchedIndex(LineIndex):
    """Line index implemented with line batches."""

    def __init__(self, data: BatchKeyData, storage: LineBatchedStorage):
        """
        Initialize LineBatchIndex with batch key data and line batched storage.

        :param data: Batch key data.
        :param storage: Line batched storage.
        """
        self.__data: BatchKeyData = data
        self.__storage: LineBatchedStorage = storage

    def has(self, line: str) -> bool:
        line += "\n"
        if line < self.__data.batch_start_lines[0] or self.__data.last_line < line:
            return False
        batch_number = LineBatchedIndex.__search_binary(
            line, self.__data.batch_start_lines
        )
        # TODO?: Find the line using file.seek() instead or reading all lines.
        file_lines = self.__storage.get(batch_number)
        line_index = LineBatchedIndex.__search_binary(line, file_lines)
        return line == file_lines[line_index]

    @staticmethod
    def __search_binary(line: str, lines: List[str]) -> int:
        """
        Perform binary search on a sorted list of lines.

        :param line: The line to search for.
        :param lines: The sorted list of lines.
        :return: The index of the found line.
        """
        left_index, right_index = 0, len(lines) - 1
        while left_index < right_index:
            mid_index = (left_index + right_index + 1) // 2
            if line < lines[mid_index]:
                right_index = mid_index - 1
            else:
                left_index = mid_index
        return left_index
