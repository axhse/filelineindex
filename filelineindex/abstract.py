from abc import ABC, abstractmethod
from typing import Optional


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


class LineBatch(ABC):
    def __contains__(self, line: str) -> bool:
        """
        Check if a specific line is present in the batch.

        :param line: The line to check.
        :return: True if the line is present, False otherwise.
        """
        return self.has(line)

    @property
    @abstractmethod
    def smallest(self) -> Optional[str]:
        """
        Get the smallest line of the batch.

        :return: The last smallest of the batch, or None if the batch is empty.
        """
        pass

    @property
    @abstractmethod
    def greatest(self) -> Optional[str]:
        """
        Get the greatest line of the batch.

        :return: The greatest line of the batch, or None if the batch is empty.
        """
        pass

    @abstractmethod
    def has(self, line: str) -> bool:
        """
        Check if a specific line is present in the batch.

        :param line: The line to check.
        :return: True if the line is present, False otherwise.
        """
        pass

    @abstractmethod
    def lower(self, line: str) -> Optional[str]:
        """
        Find the lexicographically greatest line of the batch
        that is not lexicographically greater than a specified line.

        :param line: The specified line.
        :return: The line if found, None if all lines are lexicographically greater than the specified line.
        """
        pass

    @abstractmethod
    def upper(self, line: str) -> Optional[str]:
        """
        Find the lexicographically smallest line of the batch
        that is not lexicographically smaller than a specified line.

        :param line: The specified line.
        :return: The line if found, None if all lines are lexicographically smaller than the specified line.
        """
        pass


class LineBatchedStorage(ABC):
    """Abstract base class for line batched storage implementations."""

    def __getitem__(self, batch_number: int) -> LineBatch:
        """
        Get a batch by its number.

        :param batch_number: The batch number to retrieve.
        :return: The batch.
        """
        return self.get(batch_number)

    @abstractmethod
    def get(self, batch_number: int) -> LineBatch:
        """
        Get a batch by its number.

        :param batch_number: The batch number to retrieve.
        :return: The batch.
        """
        pass
