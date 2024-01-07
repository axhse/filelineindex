from typing import Callable, Generator, List, Optional, Tuple

from filelineindex.core.batched_index import BatchKeyData, LineBatchedIndex, LineIndex
from filelineindex.core.batched_storage import FileLineBatchedStorage
from filelineindex.core.filetools import (
    RECOMMENDED_OS_FILE_LIMIT,
    UTF_8,
    convert_file_number,
    join_paths,
    make_empty_dir,
    read_lines,
    remove_dir,
    size_of_line,
    write,
    yield_from_files,
)
from filelineindex.progress import Progress, VoidProgress


class IndexerOptions:
    """Options for configuring the behavior of the Indexer."""

    FILE_COUNT_LIMITS: Tuple[int, int] = (1, RECOMMENDED_OS_FILE_LIMIT)
    DEFAULT_MIN_FILE_COUNT: int = 1
    DEFAULT_MAX_FILE_COUNT: int = 1_000_000

    def __init__(
        self,
        min_file_count: int = DEFAULT_MIN_FILE_COUNT,
        max_file_count: int = DEFAULT_MAX_FILE_COUNT,
        wanted_file_count: Optional[int] = None,
    ):
        """
        Initialize IndexerOptions.

        :param min_file_count: Minimum allowed file count.
        :param max_file_count: Maximum allowed file count.
        :param wanted_file_count: Wanted file count.
        """
        self.__min_file_count: int = min_file_count
        self.__max_file_count: int = max_file_count
        self.__wanted_file_count: Optional[int] = wanted_file_count
        self.__raise_if_is_not_valid()

    @property
    def min_file_count(self) -> int:
        """Gets the minimum allowed file count."""
        return self.__min_file_count

    @property
    def max_file_count(self) -> int:
        """Gets the maximum allowed file count."""
        return self.__max_file_count

    @property
    def wanted_file_count(self) -> Optional[int]:
        """Gets the wanted file count."""
        return self.__wanted_file_count

    def __raise_if_is_not_valid(self) -> None:
        limits = IndexerOptions.FILE_COUNT_LIMITS
        counts = [
            self.__wanted_file_count,
            self.__min_file_count,
            self.__max_file_count,
        ]
        for count in counts:
            if count is not None and (count < limits[0] or limits[1] < count):
                message = f"The file count must be within the range of {limits[0]} to {limits[1]}."
                raise ValueError(message)
        if self.__max_file_count < self.__min_file_count:
            message = "The minimum file count must be less than the maximum file count."
            raise ValueError(message)
        if counts[0] is not None and (counts[0] < counts[1] or counts[2] < counts[0]):
            message = "The wanted file count should be within the range of the minimum to maximum file count."
            raise ValueError(message)


class Indexer:
    """Indexer class for creating and managing index instances."""

    def __init__(self, resource_dir: str, options: IndexerOptions = IndexerOptions()):
        """
        Initialize the Indexer.

        :param resource_dir: The directory where the indexer stores its data.
        :param options: Options for configuring the behavior of the indexer.
        """
        self.__resource_dir: str = resource_dir
        self.__options: IndexerOptions = options

    @staticmethod
    def build_index_for_data(data_file_paths: List[str]) -> LineIndex:
        """
        Builds an index for existing index data.

        :param data_file_paths: List of paths to ordered data files.
        :return: An index built from the data files.
        :raises ValueError: If no data files or a file has no lines.
        """
        if len(data_file_paths) == 0:
            raise ValueError("No data files.")
        start_lines = list()
        for file_path in data_file_paths:
            with open(file_path, "r", encoding=UTF_8) as file:
                line = file.readline()
                if line == "":
                    raise ValueError("A file has no lines.")
                start_lines.append(line)
        with open(data_file_paths[-1], "r", encoding=UTF_8) as file:
            for line in file:
                last_line = line
        index_data = BatchKeyData(last_line, start_lines)
        storage = FileLineBatchedStorage(data_file_paths)
        return LineBatchedIndex(index_data, storage)

    def delete_index_data(self) -> None:
        """Delete index data by removing associated resources."""
        remove_dir(self.__resource_dir)

    def index(
        self, file_paths: List[str], progress: Progress = VoidProgress()
    ) -> LineIndex:
        """
        Creates an index for a set of input files.
        The system must have enough remaining space to store the result (as much as input data size).

        **Note: Each file must contain sorted lines, and lines in files with lower indices
        must be lexicographically smaller than those in files with higher indices.**

        Note: The system must have enough remaining space to store the result (as much as input data size).

        :param file_paths: List of paths to input files.
        :param progress: An instance of the Progress class to track and report progress. Default is VoidProgress.
        :return: An index created for the input files.
        :raises ValueError: If no files to index.
        """
        if len(file_paths) == 0:
            raise ValueError("No files to index.")
        make_empty_dir(self.__resource_dir)
        return self.__index(lambda: yield_from_files(file_paths), progress)

    def index_lines(
        self, lines: List[str], progress: Progress = VoidProgress()
    ) -> LineIndex:
        """
        Create an index for a set of input lines.
        The system must have enough remaining space to store the result (as much as input data size).

        **Note: The input lines must be sorted.**

        Note: The system must have enough remaining space to store the result (as much as input data size).

        :param lines: List of input lines.
        :param progress: An instance of the Progress class to track and report progress. Default is VoidProgress.
        :return: An index created for the input lines.
        :raises ValueError: If no lines to index.
        """

        def __yield_lines() -> Generator[str, None, None]:
            for line in lines:
                if line[-1] != "\n":
                    line += "\n"
                yield line

        make_empty_dir(self.__resource_dir)
        if len(lines) == 0:
            raise ValueError("No lines to index.")
        return self.__index(lambda: __yield_lines(), progress)

    def load_index(self) -> LineIndex:
        """
        Build an index using current resources.

        :return: Built index.
        """
        index_data = self.__read_index_data()
        batch_storage = FileLineBatchedStorage(self.__read_storage_data())
        return LineBatchedIndex(index_data, batch_storage)

    def __find_optimal_file_number(self, line_count: int, total_size: int) -> int:
        # TODO: Replace to a formula.
        file_number = 1000
        file_number = self.__options.wanted_file_count or file_number
        file_number = max(file_number, self.__options.min_file_count)
        file_number = min(file_number, line_count, self.__options.max_file_count)
        return file_number

    def __index(
        self,
        line_generator_creator: Callable[[], Generator[str, None, None]],
        progress: Progress,
    ) -> LineIndex:
        progress.report_start()
        line_count = 0
        total_size = 0
        for line in line_generator_creator():
            line_count += 1
            total_size += size_of_line(line)
        if line_count == 0:
            raise ValueError("No lines to index.")
        optimal_file_number = self.__find_optimal_file_number(line_count, total_size)
        processed_size = 0
        last_line = ""
        start_lines = list()
        try:
            start_lines.append(next(line_generator_creator()))
        except StopIteration:
            pass
        line_generator = line_generator_creator()
        file_paths = list()
        for file_number in range(optimal_file_number):
            current_limit = total_size * (file_number + 1) // optimal_file_number
            file_name = f"{convert_file_number(file_number, optimal_file_number)}.dat"
            file_path = join_paths(self.__resource_dir, file_name)
            file_paths.append(file_path)
            with open(file_path, "w", encoding=UTF_8) as data_file:
                if file_number != 0:
                    start_lines.append(last_line)
                data_file.write(last_line)
                try:
                    while True:
                        last_line = next(line_generator)
                        processed_size += size_of_line(last_line)
                        if processed_size > current_limit:
                            break
                        data_file.write(last_line)
                except StopIteration:
                    index_data = BatchKeyData(last_line, start_lines)
                    storage = FileLineBatchedStorage(file_paths)
                    self.__write_index_data(index_data)
                    self.__write_storage_data(file_paths)
                    progress.report_done()
                    return LineBatchedIndex(index_data, storage)
            progress.report((file_number + 1) / optimal_file_number)
        raise RuntimeError("An unexpected algorithm branch.")

    def __read_index_data(self) -> BatchKeyData:
        index_data_path = join_paths(self.__resource_dir, ".index")
        with open(index_data_path, "r", encoding=UTF_8) as index_file:
            last_line = index_file.readline()
            index_file.readline()
            return BatchKeyData(last_line, index_file.readlines())

    def __read_storage_data(self) -> List[str]:
        storage_data_path = join_paths(self.__resource_dir, ".storage")
        return [line[:-1] for line in read_lines(storage_data_path)]

    def __write_index_data(self, data: BatchKeyData) -> None:
        index_data_path = join_paths(self.__resource_dir, ".index")
        write(index_data_path, data.last_line)
        write(index_data_path, f"{len(data.batch_start_lines)}\n", append=True)
        write(index_data_path, data.batch_start_lines, append=True)

    def __write_storage_data(self, paths: List[str]) -> None:
        storage_data_path = join_paths(self.__resource_dir, ".storage")
        write(storage_data_path, [path + "\n" for path in paths])
