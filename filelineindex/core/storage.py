from typing import List

from filelineindex.core.abstract import LineBatchStorage


class FileLineBatchStorage(LineBatchStorage):
    def __init__(self, file_paths: List[str]):
        self.__file_paths: List[str] = file_paths

    def get(self, batch_number: int) -> List[str]:
        with open(self.__file_paths[batch_number], "r") as data_file:
            return data_file.readlines()
