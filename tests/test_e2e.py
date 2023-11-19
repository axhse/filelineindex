import os
from typing import List

import pytest

from filelineindex.core.filetools import FileTools, join_paths
from filelineindex.indexer import Indexer, IndexerOptions

TEMP_DIR = "temp"
CLEANUP_AFTER = True


@pytest.fixture()
def temp_dir(request):
    if CLEANUP_AFTER:
        request.addfinalizer(lambda: FileTools.remove_dir(TEMP_DIR))
    FileTools.make_empty_dir(TEMP_DIR)
    return TEMP_DIR


def test_indexer(temp_dir):
    indexer = Indexer(temp_dir, IndexerOptions(wanted_file_count=3))
    indexer.index(["2", "2", "4", "1", "4", "5", "3"])
    assert_has_content(temp_dir, "5\n3\n1\n2\n4\n", ["1\n", "2\n3\n", "4\n5\n"])


def test_indexer_on_files(temp_dir):
    lines = [["2", "2"], ["4"], ["1", "4", "5", "3"]]
    input_dir = join_paths(temp_dir, "input")
    output_dir = join_paths(temp_dir, "output")
    FileTools.make_empty_dir(input_dir)
    FileTools.make_empty_dir(output_dir)
    input_file_paths = list()
    for index, lines in enumerate(lines):
        lines = [line + "\n" for line in lines]
        input_file_path = join_paths(input_dir, str(index))
        input_file_paths.append(input_file_path)
        FileTools.write_lines(lines, input_file_path)
    indexer = Indexer(output_dir, IndexerOptions(wanted_file_count=3))
    indexer.index_files(input_file_paths)
    assert_has_content(output_dir, "5\n3\n1\n2\n4\n", ["1\n", "2\n3\n", "4\n5\n"])


def test_index(temp_dir):
    lines = [str(n) for n in range(2, 8)]
    indexer = Indexer(temp_dir, IndexerOptions(wanted_file_count=3))
    indexer.index(lines)
    index = indexer.load_index()
    for line in sorted(set(lines)):
        assert index.has(line)
    assert not index.has(str(1))
    assert not index.has(str(8))
    assert not index.has("")
    assert not index.has(lines[0] + "\n")


def assert_has_content(
    resource_dir: str, index_content: str, data_content: List[str]
) -> None:
    _, dirs, file_paths = next(os.walk(resource_dir))
    assert 0 == len(dirs)
    data_file_paths = {f"{number}.dat" for number in range(len(data_content))}
    expected_paths = {".index"} | data_file_paths
    assert expected_paths == set(file_paths)
    index_path = join_paths(resource_dir, ".index")
    assert index_content == FileTools.read(index_path)
    for file_number, expected_content in enumerate(data_content):
        data_path = join_paths(resource_dir, f"{file_number}.dat")
        assert expected_content == FileTools.read(data_path)
