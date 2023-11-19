import os
import shutil
from typing import Iterable, List


def join_paths(*paths):
    return os.path.join(*paths)


class FileTools:
    @staticmethod
    def clear_dir(path: str) -> None:
        if os.path.isdir(path):
            _, dir_paths, file_paths = next(os.walk(path))
            for dir_path in dir_paths:
                shutil.rmtree(join_paths(path, dir_path))
            for file_path in file_paths:
                os.remove(join_paths(path, file_path))

    @staticmethod
    def make_dir(path: str) -> None:
        if not os.path.isdir(path):
            os.mkdir(path)

    @staticmethod
    def make_empty_dir(path: str) -> None:
        FileTools.make_dir(path)
        FileTools.clear_dir(path)

    @staticmethod
    def remove_dir(path: str) -> None:
        if os.path.isdir(path):
            shutil.rmtree(path)

    @staticmethod
    def read(path: str) -> str:
        with open(path, "r") as file:
            return file.read()

    @staticmethod
    def read_lines(path: str) -> List[str]:
        with open(path, "r") as file:
            return file.readlines()

    @staticmethod
    def write_line(line: str, path: str, append=False) -> None:
        FileTools.write_lines([line], path, append)

    @staticmethod
    def write_lines(lines: Iterable[str], path: str, append=False) -> None:
        mode = "a" if append else "w"
        with open(path, mode) as file:
            file.writelines(lines)
