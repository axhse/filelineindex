from enum import IntEnum, auto
from typing import Callable, Generator, Iterable, List, Optional, Tuple

from filelineindex.core.filetools import FileTools, join_paths
from filelineindex.core.index import IndexData, LineIndex
from filelineindex.core.storage import FileStorage


class Preprocess(IntEnum):
    NONE = auto()
    ORDER = auto()
    SORT_AND_ORDER = auto()


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
        self.__temp_sort_dir: str = join_paths(self.__resource_dir, "sort")
        self.__temp_order_dir: str = join_paths(self.__resource_dir, "order")
        self.__options: IndexerOptions = options

    @staticmethod
    def are_valid_lines(lines: Iterable[str]) -> bool:
        return any("\n" in line and line.index("\n") < len(line) - 1 for line in lines)

    def drop_index(self) -> None:
        FileTools.remove_dir(self.__resource_dir)

    def index(self, lines: List[str], preprocess=True) -> None:
        FileTools.make_empty_dir(self.__resource_dir)
        if preprocess:
            lines = list(set(lines))
            lines.sort()
        if len(lines) == 0:
            raise ValueError("No lines to index.")
        self.__index(lambda: self.__yield_lines(lines))

    def index_files(
        self, file_paths: List[str], preprocess: Preprocess = Preprocess.SORT_AND_ORDER
    ) -> None:
        FileTools.make_empty_dir(self.__resource_dir)
        if preprocess == Preprocess.SORT_AND_ORDER:
            file_paths = self.__make_sorted_files(file_paths)
        if preprocess in (Preprocess.SORT_AND_ORDER, Preprocess.ORDER):
            file_paths = self.__make_ordered_files(file_paths)
        FileTools.remove_dir(self.__temp_sort_dir)
        self.__index(lambda: self.__yield_file_lines(file_paths))
        FileTools.remove_dir(self.__temp_order_dir)

    def load_index(self) -> LineIndex:
        batch_storage = FileStorage(self.__resource_dir)
        return LineIndex(self.__read_index_data(), batch_storage)

    @staticmethod
    def __yield_lines(lines: Iterable[str]) -> Generator[str, None, None]:
        for line in lines:
            if line[-1] != "\n":
                line += "\n"
            yield line

    @staticmethod
    def __yield_file_lines(file_paths: Iterable[str]) -> Generator[str, None, None]:
        for file_path in file_paths:
            with open(file_path, "r") as file:
                yield from file

    def __find_optimal_file_count(self, line_count: int, symbol_count: int) -> int:
        # TODO: Replace to a formula.
        file_count = 1000
        file_count = self.__options.wanted_file_count or file_count
        file_count = max(file_count, self.__options.min_file_count)
        file_count = min(file_count, line_count, self.__options.max_file_count)
        return file_count

    def __index(
        self,
        generator_creator: Callable[[], Generator[str, None, None]],
    ) -> None:
        line_count = 0
        symbol_count = 0
        for line in generator_creator():
            line_count += 1
            symbol_count += len(line)
        if line_count == 0:
            raise ValueError("No lines to index.")
        file_count = self.__find_optimal_file_count(line_count, symbol_count)
        generator = generator_creator()
        line_index = 0
        start_lines = list()
        for file_number in range(file_count):
            index_limit = line_count * (file_number + 1) // file_count
            lines = list()
            while line_index < index_limit:
                line_index += 1
                lines.append(next(generator))
            start_lines.append(lines[0])
            self.__write_indexed_lines(lines, file_number)
            if file_number + 1 == file_count:
                self.__write_index_data(IndexData(lines[-1], start_lines))

    def __make_sorted_files(self, file_paths: Iterable[str]) -> List[str]:
        FileTools.make_empty_dir(self.__temp_sort_dir)
        new_file_paths = list()
        for file_number, file_path in enumerate(file_paths):
            temp_file_path = join_paths(self.__temp_sort_dir, str(file_number))
            new_file_paths.append(temp_file_path)
            with open(file_path, "r") as file:
                FileTools.write_lines(sorted(set(file)), temp_file_path)
        return new_file_paths

    def __make_ordered_files(self, file_paths: Iterable[str]) -> List[str]:
        # TODO: Optimize using tree-based structures.
        FileTools.make_empty_dir(self.__temp_order_dir)
        new_file_paths = list()
        values_with_generators = list()
        total_line_count = sum(1 for _ in self.__yield_file_lines(file_paths))
        generators = [self.__yield_file_lines([path]) for path in file_paths]
        for generator in generators:
            try:
                value = next(generator)
                values_with_generators.append([value, generator])
            except StopIteration:
                continue
        values_with_generators.sort(key=lambda item: item[0])
        current_line_index = 0
        last_value = None
        for file_number in range(len(generators)):
            current_limit = total_line_count * (file_number + 1) // len(generators)
            temp_file_path = join_paths(self.__temp_order_dir, str(file_number))
            new_file_paths.append(temp_file_path)
            with open(temp_file_path, "w") as temp_file:
                while current_line_index < current_limit:
                    value = values_with_generators[0][0]
                    if value != last_value:
                        last_value = value
                        temp_file.write(value)
                        current_line_index += 1
                    try:
                        value = next(values_with_generators[0][1])
                        while value == last_value:
                            value = next(values_with_generators[0][1])
                        last_value = value
                        values_with_generators[0][0] = value
                        values_with_generators.sort(key=lambda item: item[0])
                    except StopIteration:
                        values_with_generators.pop(0)
                        if len(values_with_generators) == 0:
                            return new_file_paths
        return new_file_paths

    def __read_index_data(self) -> IndexData:
        index_path = join_paths(self.__resource_dir, ".index")
        with open(index_path, "r") as index_file:
            last_line = index_file.readline()
            index_file.readline()
            return IndexData(last_line, index_file.readlines())

    def __write_index_data(self, data: IndexData) -> None:
        index_path = join_paths(self.__resource_dir, ".index")
        FileTools.write_line(data.last_line, index_path)
        FileTools.write_line(f"{len(data.start_lines)}\n", index_path, append=True)
        FileTools.write_lines(data.start_lines, index_path, append=True)

    def __write_indexed_lines(self, lines: List[str], file_number: int) -> None:
        data_path = join_paths(self.__resource_dir, f"{file_number}.dat")
        FileTools.write_lines(lines, data_path)
