from abc import ABC, abstractmethod


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
