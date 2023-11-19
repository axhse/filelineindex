from abc import ABC, abstractmethod
from typing import List


class LineBatchStorage(ABC):
    @abstractmethod
    def get(self, batch_number: int) -> List[str]:
        pass
