import os
import shutil
from typing import Iterable, List, Union

UTF_8: str = "utf-8"


class FileSize:
    def __init__(self, b: int = 0, kb: int = 0, mb: int = 0, gb: int = 0):
        if b < 0 or kb < 0 or mb < 0 or gb < 0:
            raise ValueError("Invalid size values: must not be negative.")
        self.__total_bytes = b + 2**10 * kb + 2**20 * mb + 2**30 * gb

    def __int__(self) -> int:
        return self.__total_bytes

    @property
    def total_bytes(self) -> int:
        return self.__total_bytes


def sizeof(line: str) -> int:
    return len(line.encode(UTF_8))


def join_paths(*paths: str) -> str:
    return os.path.join(*paths)


def get_basename(path: str) -> str:
    return os.path.basename(path)


def get_parent_path(path: str) -> str:
    return join_paths(*os.path.split(path)[:-1])


def count_bytes(paths: Union[str, Iterable[str]]) -> int:
    paths = [paths] if type(paths) == str else paths
    result = 0
    for path in paths:
        with open(path, "r", encoding=UTF_8) as file:
            result += sum(len(line) for line in file)
    return result


def count_lines(paths: Union[str, Iterable[str]]) -> int:
    paths = [paths] if type(paths) == str else paths
    result = 0
    for path in paths:
        with open(path, "r", encoding=UTF_8) as file:
            result += sum(1 for _ in file)
    return result


def clear_dir(path: str) -> None:
    if os.path.isdir(path):
        _, dir_paths, file_paths = next(os.walk(path))
        for dir_path in dir_paths:
            shutil.rmtree(join_paths(path, dir_path))
        for file_path in file_paths:
            os.remove(join_paths(path, file_path))


def make_dir(path: str) -> None:
    if not os.path.isdir(path):
        os.mkdir(path)


def make_empty_dir(path: str) -> None:
    make_dir(path)
    clear_dir(path)


def remove_dir(path: str) -> None:
    if os.path.isdir(path):
        shutil.rmtree(path)


def remove_file(path: str) -> None:
    if os.path.exists(path) and not os.path.isdir(path):
        os.remove(path)


def remove_files(paths: Iterable[str]) -> None:
    for path in paths:
        remove_file(path)


def read(path: str) -> str:
    with open(path, "r", encoding=UTF_8) as file:
        return file.read()


def read_lines(path: str) -> List[str]:
    with open(path, "r", encoding=UTF_8) as file:
        return file.readlines()


def write(lines: Union[str, Iterable[str]], path: str, append=False) -> None:
    mode = "a" if append else "w"
    with open(path, mode, encoding=UTF_8) as file:
        if type(lines) == str:
            file.write(lines)
        else:
            file.writelines(lines)
