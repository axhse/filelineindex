import os
from typing import List

from filelineindex.core.filetools import join_paths, read
from filelineindex.indexer import Indexer, IndexerOptions
from tests.shared import assert_has_content, get_input_paths, temp_dir


def test_line_indexing(temp_dir):
    indexer = Indexer(temp_dir, IndexerOptions(wanted_file_count=3))
    index = indexer.index_lines([str(n) for n in range(1, 5 + 1)])
    assert "5" in index
    assert "6" not in index
    assert_has_index_content(temp_dir, "5\n3\n1\n2\n4\n", ["1\n", "2\n3\n", "4\n5\n"])


def test_file_indexing(temp_dir):
    input_paths = get_input_paths("files_to_index")
    indexer = Indexer(temp_dir, IndexerOptions(wanted_file_count=3))
    index = indexer.index(input_paths)
    assert "5" in index
    assert "6" not in index
    assert_has_index_content(
        temp_dir, "5\n3\n1\n200\n4\n", ["1\n", "200\n3\n", "4\n5\n"]
    )


def test_index(temp_dir):
    lines = [str(n) for n in range(2, 8)]
    indexer = Indexer(temp_dir, IndexerOptions(wanted_file_count=3))
    returned_index = indexer.index_lines(lines)
    loaded_index = indexer.load_index()
    for index in (returned_index, loaded_index):
        for line in lines:
            assert line in index
        assert str(1) not in index
        assert str(8) not in index
        assert "" not in index
        assert (sorted(lines)[0] + "\n") not in index


def test_index_build_for_data(temp_dir):
    input_paths = get_input_paths("files_to_index")
    index = Indexer.build_index_for_data(input_paths)
    assert_has_content(temp_dir, dict())
    lines = ["1", "200", "3", "4", "5"]
    for line in lines:
        assert line in index
    assert str(0) not in index
    assert str(8) not in index
    assert "" not in index
    assert (sorted(lines)[0] + "\n") not in index


def assert_has_index_content(
    resource_dir: str, index_content: str, data_content: List[str]
) -> None:
    _, dirs, file_paths = next(os.walk(resource_dir))
    assert 0 == len(dirs)
    data_file_paths = {f"{number}.dat" for number in range(len(data_content))}
    expected_file_paths = {".index", ".storage"} | data_file_paths
    assert expected_file_paths == set(file_paths)
    index_path = join_paths(resource_dir, ".index")
    assert index_content == read(index_path)
    storage_path = join_paths(resource_dir, ".storage")
    storage_content = "".join(
        join_paths(resource_dir, f"{n}.dat") + "\n" for n in range(len(data_content))
    )
    assert storage_content == read(storage_path)
    for file_number, expected_content in enumerate(data_content):
        data_file_path = join_paths(resource_dir, f"{file_number}.dat")
        assert expected_content == read(data_file_path)
