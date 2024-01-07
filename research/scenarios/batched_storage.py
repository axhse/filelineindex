from filelineindex.indexer import Indexer, IndexerOptions
from research.benchmark import benchmark_line_check
from research.scenarios.configuration import (
    INDEX_RESOURCE_DIR,
    LINES_TO_CHECK,
    PREPROCESSED_FILES,
)

if __name__ == "__main__":
    options = IndexerOptions()
    indexer = Indexer(INDEX_RESOURCE_DIR, options)
    index = indexer.index(PREPROCESSED_FILES)
    benchmark_line_check(1, index, LINES_TO_CHECK)
