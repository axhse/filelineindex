from filelineindex.abstract import LineIndex
from filelineindex.core.algorithm import bs_has


class InMemoryIndex(LineIndex):
    def __init__(self, lines):
        self.__lines = lines

    def has(self, line: str) -> bool:
        return bs_has(line + "\n", self.__lines)
