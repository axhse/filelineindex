from abc import ABC, abstractmethod
from typing import List


class LineIndex(ABC):
    """Abstract base class for line index implementations."""

    def __contains__(self, line: str) -> bool:
        """
        Check if a specific line is present in the index.

        :param line: The line to check.
        :return: True if the line is present, False otherwise.
        """
        return self.has(line)

    @abstractmethod
    def has(self, line: str) -> bool:
        """
        Check if a specific line is present in the index.

        :param line: The line to check.
        :return: True if the line is present, False otherwise.
        """
        pass


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
