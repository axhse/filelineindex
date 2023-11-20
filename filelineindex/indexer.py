from typing import Callable, Generator, Iterable, List, Optional, Tuple

from filelineindex.core.filetools import (
    UTF_8,
    join_paths,
    make_empty_dir,
    read_lines,
    remove_dir,
    sizeof,
    write,
)
from filelineindex.core.index import IndexData, LineBatchIndex, LineIndex
from filelineindex.core.storage import FileLineBatchStorage


class IndexerOptions:
    FILE_COUNT_LIMITS: Tuple[int, int] = (1, 1_000_000_000)
    DEFAULT_MIN_FILE_COUNT: int = 1
    DEFAULT_MAX_FILE_COUNT: int = 1_000_000

    def __init__(
        self,
        min_file_count: int = DEFAULT_MIN_FILE_COUNT,
        max_file_count: int = DEFAULT_MAX_FILE_COUNT,
        wanted_file_count: Optional[int] = None,
    ):
        self.__min_file_count: int = min_file_count
        self.__max_file_count: int = max_file_count
        self.__wanted_file_count: Optional[int] = wanted_file_count
        self.__raise_if_is_not_valid()

    @property
    def min_file_count(self) -> int:
        return self.__min_file_count

    @property
    def max_file_count(self) -> int:
        return self.__max_file_count

    @property
    def wanted_file_count(self) -> Optional[int]:
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
    def __init__(self, resource_dir: str, options: IndexerOptions = IndexerOptions()):
        self.__resource_dir: str = resource_dir
        self.__options: IndexerOptions = options

    @staticmethod
    def build_index_for_data(data_file_paths: List[str]) -> LineIndex:
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
        index_data = IndexData(last_line, start_lines)
        storage = FileLineBatchStorage(data_file_paths)
        return LineBatchIndex(index_data, storage)

    def drop_index(self) -> None:
        remove_dir(self.__resource_dir)

    def index(self, file_paths: List[str]) -> LineIndex:
        if len(file_paths) == 0:
            raise ValueError("No files to index.")
        make_empty_dir(self.__resource_dir)
        return self.__index(lambda: self.__yield_file_lines(file_paths))

    def index_lines(self, lines: List[str]) -> LineIndex:
        make_empty_dir(self.__resource_dir)
        if len(lines) == 0:
            raise ValueError("No lines to index.")
        return self.__index(lambda: self.__yield_lines(lines))

    def load_index(self) -> LineIndex:
        index_data = self.__read_index_data()
        batch_storage = FileLineBatchStorage(self.__read_storage_data())
        return LineBatchIndex(index_data, batch_storage)

    @staticmethod
    def __yield_file_lines(file_paths: Iterable[str]) -> Generator[str, None, None]:
        for file_path in file_paths:
            with open(file_path, "r") as file:
                yield from file

    @staticmethod
    def __yield_lines(lines: Iterable[str]) -> Generator[str, None, None]:
        for line in lines:
            if line[-1] != "\n":
                line += "\n"
            yield line

    def __find_optimal_file_count(self, line_count: int, total_size: int) -> int:
        # TODO: Replace to a formula.
        file_count = 1000
        file_count = self.__options.wanted_file_count or file_count
        file_count = max(file_count, self.__options.min_file_count)
        file_count = min(file_count, line_count, self.__options.max_file_count)
        return file_count

    def __index(
        self,
        line_generator_creator: Callable[[], Generator[str, None, None]],
    ) -> LineIndex:
        line_count = 0
        total_size = 0
        for line in line_generator_creator():
            line_count += 1
            total_size += sizeof(line)
        if line_count == 0:
            raise ValueError("No lines to index.")
        optimal_file_count = self.__find_optimal_file_count(line_count, total_size)
        processed_size = 0
        last_line = ""
        start_lines = list()
        try:
            start_lines.append(next(line_generator_creator()))
        except StopIteration:
            pass
        line_generator = line_generator_creator()
        file_paths = list()
        for file_number in range(optimal_file_count):
            current_limit = total_size * (file_number + 1) // optimal_file_count
            file_path = join_paths(self.__resource_dir, f"{file_number}.dat")
            file_paths.append(file_path)
            with open(file_path, "w", encoding=UTF_8) as data_file:
                if file_number != 0:
                    start_lines.append(last_line)
                data_file.write(last_line)
                try:
                    while True:
                        last_line = next(line_generator)
                        processed_size += sizeof(last_line)
                        if processed_size > current_limit:
                            break
                        data_file.write(last_line)
                except StopIteration:
                    index_data = IndexData(last_line, start_lines)
                    storage = FileLineBatchStorage(file_paths)
                    self.__write_index_data(index_data)
                    self.__write_storage_data(file_paths)
                    return LineBatchIndex(index_data, storage)
        raise RuntimeError("An unexpected algorithm branch.")

    def __read_index_data(self) -> IndexData:
        index_data_path = join_paths(self.__resource_dir, ".index")
        with open(index_data_path, "r", encoding=UTF_8) as index_file:
            last_line = index_file.readline()
            index_file.readline()
            return IndexData(last_line, index_file.readlines())

    def __read_storage_data(self) -> List[str]:
        storage_data_path = join_paths(self.__resource_dir, ".storage")
        return [line[:-1] for line in read_lines(storage_data_path)]

    def __write_index_data(self, data: IndexData) -> None:
        index_data_path = join_paths(self.__resource_dir, ".index")
        write(data.last_line, index_data_path)
        write(f"{len(data.start_lines)}\n", index_data_path, append=True)
        write(data.start_lines, index_data_path, append=True)

    def __write_storage_data(self, paths: List[str]) -> None:
        storage_data_path = join_paths(self.__resource_dir, ".storage")
        write([path + "\n" for path in paths], storage_data_path)
