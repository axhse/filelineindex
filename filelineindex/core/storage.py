from typing import List

from filelineindex.core.abstract import LineBatchStorage
from filelineindex.core.filetools import join_paths


class FileStorage(LineBatchStorage):
    def __init__(self, resource_dir: str):
        self.__resource_dir: str = join_paths(resource_dir, "")

    def get(self, batch_number: int) -> List[str]:
        file_path = f"{self.__resource_dir}{batch_number}.dat"
        with open(file_path, "r") as data_file:
            return data_file.readlines()
