from abc import ABC, abstractmethod
from typing import List


class LineBatchedStorage(ABC):
    """Abstract base class for line batched storage implementations."""

    def __getitem__(self, batch_number: int) -> List[str]:
        """
        Get a batch of lines based on the batch number.

        :param batch_number: The batch number to retrieve.
        :return: A list of lines belonging to the specified batch.
        """
        return self.get(batch_number)

    @abstractmethod
    def get(self, batch_number: int) -> List[str]:
        """
        Get a batch of lines based on the batch number.

        :param batch_number: The batch number to retrieve.
        :return: A list of lines belonging to the specified batch.
        """
        pass


class FileLineBatchedStorage(LineBatchedStorage):
    def __init__(self, file_paths: List[str]):
        self.__file_paths: List[str] = file_paths

    def get(self, batch_number: int) -> List[str]:
        with open(self.__file_paths[batch_number], "r") as data_file:
            return data_file.readlines()
