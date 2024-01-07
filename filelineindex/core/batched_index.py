from typing import List

from filelineindex.abstract import LineIndex
from filelineindex.core.algorithm import bs_lower_index
from filelineindex.core.batched_storage import LineBatchedStorage


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
        batch_number = bs_lower_index(line, self.__data.batch_start_lines)
        if batch_number is None:
            return False
        return self.__storage.get(batch_number).has(line)
