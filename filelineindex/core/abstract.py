from abc import ABC, abstractmethod
from typing import List


class LineIndex(ABC):
    @abstractmethod
    def __contains__(self, line: str) -> bool:
        pass

    @abstractmethod
    def has(self, line: str) -> bool:
        pass


class LineBatchStorage(ABC):
    @abstractmethod
    def get(self, batch_number: int) -> List[str]:
        pass
