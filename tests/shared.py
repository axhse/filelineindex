import os
from typing import Dict, List

import pytest

from filelineindex.core.filetools import join_paths, make_empty_dir, read, remove_dir

TEMP_DIR = "temp"
CLEANUP_AFTER = False


@pytest.fixture
def temp_dir(request) -> str:
    if CLEANUP_AFTER:
        request.addfinalizer(lambda: remove_dir(TEMP_DIR))
    make_empty_dir(TEMP_DIR)
    return TEMP_DIR


def assert_has_content(directory: str, expected_content: Dict[str, str]) -> None:
    _, dirs, file_paths = next(os.walk(directory))
    assert len(dirs) == 0
    assert set(file_paths) == set(expected_content.keys())
    for file_name, expected_text in expected_content.items():
        actual_text = read(join_paths(directory, file_name))
        assert actual_text == expected_text


def get_input_paths(dir_name: str) -> List[str]:
    file_names = next(os.walk(join_paths("input", dir_name)))[2]
    return [join_paths("input", dir_name, name) for name in file_names]
