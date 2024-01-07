from typing import Generator, Iterable, List, Optional, Tuple, Type, Union

from filelineindex.core.filetools import (
    UTF_8,
    FileSize,
    convert_file_number,
    count_bytes,
    count_lines,
    get_basename,
    get_parent_path,
    join_paths,
    make_dir,
    read,
    size_of_line,
    write,
    yield_from_file,
)
from filelineindex.progress import Progress, VoidProgress


class SplitToBatches:
    """Represents a strategy to split a file into a specified number of batches."""

    def __init__(self, number: int):
        """
        Initialize the SplitToBatches strategy with a specified number of batches.

        :param number: The number of batches to split the file into.
        """
        self.number: int = number


class SplitToSize:
    """Represents a strategy to split a file based on a specified size."""

    def __init__(self, size: FileSize):
        """
        Initialize the SplitToSize strategy with a target size.

        :param size: The target size for each split file.
        """
        self.size: FileSize = size


SplitStrategy: Type = Union[SplitToBatches, SplitToSize]


def are_valid_lines(lines: Iterable[str]) -> bool:
    """
    Check if a set of lines is valid:

    --  Each line does not contain newline characters.

    :param lines: Iterable of lines to check.
    :return: True if all lines are valid, False otherwise.
    """
    return all("\n" not in line for line in lines)


def fix_lines(lines: Iterable[str]) -> List[str]:
    """
    Remove newline characters from each line in the given iterable of lines.

    :param lines: Iterable of lines to fix.
    :return: List of lines with newline characters removed.
    """
    return [line.replace("\n", "") for line in lines]


def preprocess_lines(lines: Iterable[str]) -> List[str]:
    """
    Preprocess a set of lines by removing duplicates and sorting them.

    :param lines: Iterable of lines to preprocess.
    :return: List of unique and sorted lines.
    """
    return sorted(set(lines))


def merge_files(
    input_paths: List[str], output_path: str, special_ending: str = ""
) -> None:
    """
    Merge the contents of multiple text files into a single output file.

    :param input_paths: List of paths to the input text files.
    :param output_path: Path to the output text file where the merged content will be written.
    :param special_ending: Optional string used to be written after each file contents.
                           If not provided, no special ending is written.
    """
    with open(output_path, "w", encoding=UTF_8) as output_file:
        for input_path in input_paths:
            output_file.write(read(input_path))
            output_file.write(special_ending)


def split_file(input_path: str, output_dir: str, strategy: SplitStrategy) -> List[str]:
    """
    Split a file into batches based on the specified strategy.

    :param input_path: Path to the input file.
    :param output_dir: Directory to store the output batches.
    :param strategy: Split strategy.
    :return: List of paths to the output batches.
    """

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

    line_generator = yield_from_file(input_path)
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
                processed_size = size_of_line(last_line)
                while True:
                    last_line = next(line_generator)
                    processed_size += size_of_line(last_line)
                    if processed_size > current_limit:
                        processed_size -= size_of_line(last_line)
                        break
                    output_file.write(last_line)
            file_index += 1
    except StopIteration:
        return output_paths


def is_file_sorted(file_path: str) -> bool:
    """
    Check if the lines in a text file are sorted in ascending order.

    :param file_path: The path to the text file.
    :return: True if the lines are sorted, False otherwise.
    """
    last_line = None
    with open(file_path, "r", encoding=UTF_8) as file:
        for line in file:
            if last_line is not None and line < last_line:
                return False
            last_line = line
    return True


def are_ordered_files(file_paths: List[str]) -> bool:
    """
    Check if the lines in a list of text files are ordered in ascending order across all files.

    :param file_paths: List of paths to the text files.
    :return: True if the lines are ordered, False otherwise.
    """
    previous_greatest_line = None
    current_greatest_line = None
    for file_path in file_paths:
        with open(file_path, "r", encoding=UTF_8) as file:
            for line in file:
                if current_greatest_line is None or current_greatest_line < line:
                    current_greatest_line = line
                if previous_greatest_line is not None and line < previous_greatest_line:
                    return False
        previous_greatest_line = current_greatest_line
    return True


def sort_file(input_path: str, output_path: Optional[str] = None) -> None:
    """
    Sort the lines of a single file and optionally save the result to another file.

    :param input_path: Path to the input file.
    :param output_path: Optional path to the output file. If not provided, the input file is overwritten.
    """
    output_path = output_path or input_path
    with open(input_path, "r", encoding=UTF_8) as file:
        lines = set(file)
    write(output_path, sorted(lines))


def sort_files_separately(
    input_paths: Iterable[str], output_dir: str, progress: Progress = VoidProgress()
) -> List[str]:
    """
    Sort multiple files separately and save the sorted results in the specified directory.

    Example:
    If the function is applied to input files with contents "4\n1\n2\n" and "3\n2\n", the output files will store "1\n2\n4\n" and "2\n3\n".

    :param input_paths: Iterable of paths to input files.
    :param output_dir: Directory to store the sorted output files.
    :param progress: An instance of the Progress class to track and report progress. Default is VoidProgress.
    :return: List of paths to the sorted output files.
    """
    progress.report_start()
    input_paths = list(input_paths)
    output_paths = list()
    make_dir(output_dir)
    for index, input_path in enumerate(input_paths):
        output_path = join_paths(output_dir, get_basename(input_path))
        output_paths.append(output_path)
        sort_file(input_path, output_path)
        progress.report((index + 1) / len(input_paths))
    return output_paths


def merge_sorted_files(
    input_paths: Iterable[str], output_dir: str, progress: Progress = VoidProgress()
) -> List[str]:
    """
    Sort lines from multiple input files by merging **previously separately sorted** files. Save the sorted lines as batches in the specified output directory.

    Example: If the function is applied to input files with contents "1\n2\n4\n" and "2\n3\n", the output files will store "1\n2\n" and "3\n4\n".

    Note: The system must have enough remaining space to store the result (as much as input data size).

    :param input_paths: Iterable of paths to input files.
    :param output_dir: Directory to store the result batch files.
    :param progress: An instance of the Progress class to track and report progress. Default is VoidProgress.
    :return: List of paths to the result output batch files.
    """
    progress.report_start()
    if any(get_parent_path(path) == output_dir for path in input_paths):
        raise ValueError("The output directory must differ from the input directory.")
    make_dir(output_dir)
    output_paths = list()
    values_with_generators: List[Tuple[str, Generator[str, None, None]]] = list()
    total_line_count = count_lines(input_paths)
    generators = [yield_from_file(path) for path in input_paths]
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
        file_name = f"{convert_file_number(file_number, generator_count)}.batch"
        output_path = join_paths(output_dir, file_name)
        output_paths.append(output_path)
        with open(output_path, "w") as output_file:
            while current_line_index < current_limit:
                value = values_with_generators[-1][0]
                if value != last_value:
                    last_value = value
                    output_file.write(value)
                    current_line_index += 1
                try:
                    generator = values_with_generators[-1][1]
                    value = next(generator)
                    while value == last_value:
                        value = next(generator)
                    values_with_generators.pop(-1)
                    # TODO: Optimize using tree-based structures.
                    for index in range(len(values_with_generators) + 1):
                        if (
                            index == len(values_with_generators)
                            or value > values_with_generators[index][0]
                        ):
                            values_with_generators.insert(index, (value, generator))
                            break
                except StopIteration:
                    values_with_generators.pop(-1)
                    if len(values_with_generators) == 0:
                        progress.report_done()
                        return output_paths
        progress.report((file_number + 1) / generator_count)
    progress.report_done()
    return output_paths
