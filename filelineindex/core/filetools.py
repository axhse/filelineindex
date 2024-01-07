import os
import shutil
import uuid
from typing import Generator, Iterable, List, Optional, Union

# The name of 8-bit Unicode encoding.
UTF_8: str = "utf-8"

# The recommended maximum limit for the number of files in the operating system.
RECOMMENDED_OS_FILE_LIMIT: int = 1_000_000_000


class FileSize:
    """Represents a file size."""

    def __init__(self, b: int = 0, kb: int = 0, mb: int = 0, gb: int = 0):
        """
        Initialize a FileSize object with specified size values.

        :param b: Bytes.
        :param kb: Kilobytes.
        :param mb: Megabytes.
        :param gb: Gigabytes.
        :raises ValueError: If any size values are negative.
        """
        if b < 0 or kb < 0 or mb < 0 or gb < 0:
            raise ValueError("Invalid size values: must not be negative.")
        self.__total_bytes = b + 2**10 * kb + 2**20 * mb + 2**30 * gb

    def __int__(self) -> int:
        """
        Convert FileSize object to an integer representing the total size in bytes.

        :return: Total bytes.
        """
        return self.__total_bytes

    @property
    def total_bytes(self) -> int:
        """
        Get the total size in bytes.

        :return: Total bytes.
        """
        return self.__total_bytes


def get_uuid() -> str:
    """
    Generate a random UUID.

    :return: A generated UUID string **without dashes**.
    """
    return uuid.uuid4().hex.replace("-", "")


def convert_file_number(
    number: int, group_size: int = RECOMMENDED_OS_FILE_LIMIT
) -> str:
    """
    Convert a file indexing number to a hex string with fixed length, considering file group size.

    :param number: The file indexing number to be converted.
    :param group_size: The group size.
    :return: A string representation of the file indexing number.
    """
    number_width = 0
    group_size -= 1
    while group_size > 0:
        number_width += 1
        group_size //= 16
    return hex(number)[2:].upper().rjust(number_width, "0")


def size_of_line(line: str) -> int:
    """
    Get the size of a unicode string in bytes.

    :param line: Input string.
    :return: Size of the unicode string in bytes.
    """
    return len(line.encode(UTF_8))


def join_paths(*paths: str) -> str:
    """
    Join multiple path components into a single path.

    :param paths: Path components.
    :return: Joined path.
    """
    return os.path.join(*paths)


def get_file_paths_in_dir(dir_path: str) -> List[str]:
    """
    Get a list of all file paths in a directory.

    :param dir_path: Path to the directory.
    :return: List of file paths in the directory.
    """
    if not os.path.isdir(dir_path):
        return list()
    path_root, _, file_names = next(os.walk(dir_path))
    return [os.path.join(path_root, file_name) for file_name in file_names]


def get_basename(path: str) -> str:
    """
    Get the base name of a path.

    :param path: Input path.
    :return: Base name of the path.
    """
    return os.path.basename(path)


def get_parent_path(path: str) -> str:
    """
    Get the parent directory of a path.

    :param path: Input path.
    :return: Parent directory path.
    """
    return join_paths(*os.path.split(path)[:-1])


def yield_from_files(file_paths: Iterable[str]) -> Generator[str, None, None]:
    """
    Yield lines from multiple files.

    :param file_paths: An iterable of file paths.
    :return: Line generator.
    """
    for file_path in file_paths:
        with open(file_path, "r", encoding=UTF_8) as file:
            yield from file


def yield_from_file(path: str) -> Generator[str, None, None]:
    """
    Yield lines from a single file.

    :param path: The path to the file.
    :return: Line generator.
    """
    return yield_from_files([path])


def is_file_empty(path: str) -> bool:
    """
    Check if a file is empty.

    :param path: The path to the file.
    :return: True if the file is empty, False otherwise.
    """
    return os.path.getsize(path) == 0


def count_bytes(paths: Union[str, Iterable[str]]) -> int:
    """
    Count the total number of bytes in one or more files.

    :param paths: Single file path or iterable of file paths.
    :return: Total number of bytes.
    """
    paths = [paths] if type(paths) == str else paths
    result = 0
    for path in paths:
        with open(path, "r", encoding=UTF_8) as file:
            result += sum(len(line) for line in file)
    return result


def count_lines(paths: Union[str, Iterable[str]]) -> int:
    """
    Count the total number of lines in one or more files.

    :param paths: Single file path or iterable of file paths.
    :return: Total number of lines.
    """
    paths = [paths] if type(paths) == str else paths
    result = 0
    for path in paths:
        with open(path, "r", encoding=UTF_8) as file:
            result += sum(1 for _ in file)
    return result


def clear_dir(path: str) -> None:
    """
    Clear all files and subdirectories in a directory.

    :param path: Directory path.
    """
    if os.path.isdir(path):
        _, dir_paths, file_paths = next(os.walk(path))
        for dir_path in dir_paths:
            shutil.rmtree(join_paths(path, dir_path))
        for file_path in file_paths:
            os.remove(join_paths(path, file_path))


def make_dir(path: str) -> None:
    """
    Create a directory if it does not exist.

    :param path: Directory path.
    """
    if not os.path.isdir(path):
        os.mkdir(path)


def make_empty_dir(path: str) -> None:
    """
    Create an empty directory by first making sure it exists and then clearing it.

    :param path: Directory path.
    """
    make_dir(path)
    clear_dir(path)


def remove_dir(path: str) -> None:
    """
    Remove a directory and its content.

    :param path: Directory path.
    """
    if os.path.isdir(path):
        shutil.rmtree(path)


def remove_file(path: str) -> None:
    """
    Remove a file if it exists.

    :param path: File path.
    """
    if os.path.exists(path) and not os.path.isdir(path):
        os.remove(path)


def remove_files(paths: Iterable[str]) -> None:
    """
    Remove multiple files.

    :param paths: Iterable of file paths.
    """
    for path in paths:
        remove_file(path)


def read(path: str) -> str:
    """
    Read the contents of a file.

    :param path: File path.
    :return: Contents of the file.
    """
    with open(path, "r", encoding=UTF_8) as file:
        return file.read()


def read_lines(path: str) -> List[str]:
    """
    Read the lines of a file and return them as a list.

    :param path: File path.
    :return: List of lines.
    """
    with open(path, "r", encoding=UTF_8) as file:
        return file.readlines()


def read_first_line(path: str) -> Optional[str]:
    """
    Read the first line of a file.

    :param path: File path.
    :return: The line if found, None if the file is empty.
    """
    if is_file_empty(path):
        return None
    return next(yield_from_file(path))


def write(lines: Union[str, Iterable[str]], path: str, append=False) -> None:
    """
    Write lines to a file.

    :param lines: Lines to write (either a string or an iterable of strings).
    :param path: File path.
    :param append: Whether to use append mode (default is False).
    """
    mode = "a" if append else "w"
    with open(path, mode, encoding=UTF_8) as file:
        if type(lines) == str:
            file.write(lines)
        else:
            file.writelines(lines)
