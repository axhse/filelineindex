from typing import Generator, Iterable, List, Optional, Tuple, Type, Union

from filelineindex.core.filetools import (
    UTF_8,
    FileSize,
    count_bytes,
    count_lines,
    get_basename,
    get_parent_path,
    join_paths,
    make_dir,
    remove_dir,
    sizeof,
    write,
)


class SplitToBatches:
    def __init__(self, number: int):
        self.number: int = number


class SplitToSize:
    def __init__(self, size: FileSize):
        self.size: FileSize = size


SplitStrategy: Type = Union[SplitToBatches, SplitToSize]


def are_valid_lines(lines: Iterable[str]) -> bool:
    return all("\n" not in line for line in lines)


def fix_lines(lines: Iterable[str]) -> List[str]:
    return [line.replace("\n", "") for line in lines]


def preprocess_lines(lines: Iterable[str]) -> List[str]:
    return sorted(set(lines))


def split_file(input_path: str, output_dir: str, strategy: SplitStrategy) -> List[str]:
    def _yield_from_file() -> Generator[str, None, None]:
        with open(input_path, "r", encoding=UTF_8) as file:
            for line in file:
                yield line

    def _yield_size_limit(total_byte_count: int) -> Generator[int, None, None]:
        rest_size = yield None
        if isinstance(strategy, SplitToSize):
            while True:
                yield int(strategy.size)
        else:
            for n in range(strategy.number):
                rest_size = (
                    yield rest_size
                    + total_byte_count * (n + 1) // strategy.number
                    - total_byte_count * n // strategy.number
                )

    output_paths = list()
    last_line = ""
    processed_size = 0
    current_limit = 0
    file_index = 0
    basename = get_basename(input_path)

    line_generator = _yield_from_file()
    limit_generator = _yield_size_limit(count_bytes(input_path))
    next(limit_generator)
    try:
        while True:
            current_limit = limit_generator.send(current_limit - processed_size)
            if current_limit < 0:
                raise ValueError(
                    "The strategy requires too small file size."
                    if isinstance(strategy, SplitToSize)
                    else "The strategy requires too many batches."
                )
            output_path = join_paths(output_dir, f"{basename}.{file_index}.batch")
            output_paths.append(output_path)
            with open(output_path, "w", encoding=UTF_8) as output_file:
                output_file.write(last_line)
                processed_size = sizeof(last_line)
                while True:
                    last_line = next(line_generator)
                    processed_size += sizeof(last_line)
                    if processed_size > current_limit:
                        processed_size -= sizeof(last_line)
                        break
                    output_file.write(last_line)
            file_index += 1
    except StopIteration:
        return output_paths


def sort_single_file(input_path: str, output_path: Optional[str] = None) -> None:
    output_path = output_path or input_path
    with open(input_path, "r", encoding=UTF_8) as file:
        lines = set(file)
    write(sorted(lines), output_path)


def sort_single_file_to_dir(input_paths: str, output_dir: str) -> str:
    make_dir(output_dir)
    output_path = join_paths(output_dir, get_basename(input_paths))
    sort_single_file(input_paths, output_path)
    return output_path


def sort_files_separately(input_paths: Iterable[str], output_dir: str) -> List[str]:
    return [sort_single_file_to_dir(path, output_dir) for path in input_paths]


def sort_files_together(
    input_paths: Iterable[str],
    output_dir: str,
    separate_sort_dir: Optional[str] = None,
) -> List[str]:
    def _remove_separate_sort_dir() -> None:
        if separate_sort_dir is not None:
            remove_dir(separate_sort_dir)

    def _yield_from_file(path) -> Generator[str, None, None]:
        with open(path, "r", encoding=UTF_8) as file:
            for line in file:
                yield line

    if separate_sort_dir is not None:
        input_paths = sort_files_separately(input_paths, separate_sort_dir)
    if any(get_parent_path(path) == output_dir for path in input_paths):
        raise ValueError("The output directory must differ from the input directory.")
    # TODO: Optimize using tree-based structures.
    make_dir(output_dir)
    output_paths = list()
    values_with_generators: List[Tuple[str, Generator[str, None, None]]] = list()
    total_line_count = count_lines(input_paths)
    generators = [_yield_from_file(path) for path in input_paths]
    generator_count = len(generators)
    for generator in generators:
        try:
            value = next(generator)
            values_with_generators.append((value, generator))
        except StopIteration:
            continue
    values_with_generators.sort(key=lambda item: item[0], reverse=True)
    current_line_index = 0
    last_value = None
    for file_number in range(generator_count):
        current_limit = total_line_count * (file_number + 1) // generator_count
        output_path = join_paths(output_dir, str(file_number))
        output_paths.append(output_path)
        with open(output_path, "w") as temp_file:
            while current_line_index < current_limit:
                value = values_with_generators[-1][0]
                if value != last_value:
                    last_value = value
                    temp_file.write(value)
                    current_line_index += 1
                try:
                    generator = values_with_generators[-1][1]
                    value = next(generator)
                    while value == last_value:
                        value = next(generator)
                    for index in range(generator_count):
                        if (
                            index + 1 == generator_count
                            or value > values_with_generators[index][0]
                        ):
                            values_with_generators.insert(index, (value, generator))
                            break
                except StopIteration:
                    values_with_generators.pop(-1)
                    if len(values_with_generators) == 0:
                        _remove_separate_sort_dir()
                        return output_paths
    _remove_separate_sort_dir()
    return output_paths
