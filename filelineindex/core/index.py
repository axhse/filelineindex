from typing import List

from filelineindex.core.abstract import LineBatchStorage, LineIndex


class IndexData:
    def __init__(self, last_line: str, start_lines: List[str]):
        self.last_line: str = last_line
        self.start_lines: List[str] = start_lines


class LineBatchIndex(LineIndex):
    def __init__(self, data: IndexData, batch_storage: LineBatchStorage):
        self.__data: IndexData = data
        self.__batch_storage: LineBatchStorage = batch_storage

    def __contains__(self, line: str) -> bool:
        return self.has(line)

    def has(self, line: str) -> bool:
        line += "\n"
        if line < self.__data.start_lines[0] or self.__data.last_line < line:
            return False
        batch_number = LineBatchIndex.__search_binary(line, self.__data.start_lines)
        # TODO?: Find the line using file.seek() instead or reading all lines.
        file_lines = self.__batch_storage.get(batch_number)
        line_index = LineBatchIndex.__search_binary(line, file_lines)
        return line == file_lines[line_index]

    @staticmethod
    def __search_binary(line: str, lines: List[str]) -> int:
        left_index, right_index = 0, len(lines) - 1
        while left_index < right_index:
            mid_index = (left_index + right_index + 1) // 2
            if line < lines[mid_index]:
                right_index = mid_index - 1
            else:
                left_index = mid_index
        return left_index
