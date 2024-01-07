from typing import List, Optional

from filelineindex.abstract import LineBatch, LineBatchedStorage
from filelineindex.core.algorithm import bs_has, bs_lower_index, bs_upper_index
from filelineindex.core.filetools import read_first_line, read_lines


class FileLineBatch(LineBatch):
    def __init__(self, file_path: str):
        self.__file_path: str = file_path
        self.__first_line = None
        self.__last_line = None

    @property
    def smallest(self) -> Optional[str]:
        if self.__first_line is None:
            self.__first_line = read_first_line(self.__file_path)
        return self.__first_line

    @property
    def greatest(self) -> Optional[str]:
        if self.__last_line is None:
            lines = self.__load_lines()
            self.__last_line = lines[-1] if len(lines) > 0 else None
        return self.__last_line

    def has(self, line: str) -> bool:
        # TODO?: Try to use file.seek() instead of reading all lines.
        return bs_has(line, self.__load_lines())

    def lower(self, line: str) -> Optional[str]:
        lines = self.__load_lines()
        index = bs_lower_index(line, lines)
        return None if index is None else lines[index]

    def upper(self, line: str) -> Optional[str]:
        lines = self.__load_lines()
        index = bs_upper_index(line, lines)
        return None if index is None else lines[index]

    def __load_lines(self) -> List[str]:
        return read_lines(self.__file_path)


class FileLineBatchedStorage(LineBatchedStorage):
    def __init__(self, file_paths: List[str]):
        self.__batches = [FileLineBatch(file_path) for file_path in file_paths]

    def get(self, batch_number: int) -> FileLineBatch:
        return self.__batches[batch_number]
