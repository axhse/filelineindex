from filelineindex.core.filetools import yield_from_files
from research.benchmark import benchmark_line_check
from research.models.inmemory_index import InMemoryIndex
from research.scenarios.configuration import LINES_TO_CHECK, PREPROCESSED_FILES

if __name__ == "__main__":
    lines = list(yield_from_files(PREPROCESSED_FILES))
    index = InMemoryIndex(lines)
    benchmark_line_check(1, index, LINES_TO_CHECK)
