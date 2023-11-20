from filelineindex.core.filetools import FileSize, join_paths, read
from filelineindex.preprocess import (
    SplitToBatches,
    SplitToSize,
    are_valid_lines,
    fix_lines,
    preprocess_lines,
    sort_files_separately,
    sort_files_together,
    sort_single_file,
    sort_single_file_to_dir,
    split_file,
)
from tests.shared import assert_has_content, get_input_paths, temp_dir


def test_line_validator():
    assert are_valid_lines(["", " ", "*", "1", "some word"])
    assert not are_valid_lines(["\n"])
    assert not are_valid_lines(["some word\n"])
    assert not are_valid_lines(["some \nword"])


def test_line_fixer():
    valid_lines = ["", " ", "*", "1", "some word"]
    input_lines = valid_lines + ["\n", "some word\n", "some \nword"]
    expected_lines = valid_lines + ["", "some word", "some word"]
    assert list(sorted(fix_lines(input_lines))) == list(sorted(expected_lines))


def test_line_preprocessing():
    input_lines = ["2", "2", "4", "1", "4", "6", "3"]
    expected_lines = ["1", "2", "3", "4", "6"]
    assert preprocess_lines(input_lines) == expected_lines


def test_file_split_to_batches(temp_dir):
    expected_content = {
        "file_to_split.0.batch": "line1\n",
        "file_to_split.1.batch": "line2\n\n",
        "file_to_split.2.batch": "\nline3\n",
    }
    input_path = join_paths("input", "file_to_split")
    strategy = SplitToBatches(3)
    split_file(input_path, temp_dir, strategy)
    assert_has_content(temp_dir, expected_content)


def test_file_split_to_sizes(temp_dir):
    expected_content = {
        "file_to_split.0.batch": "line1\n",
        "file_to_split.1.batch": "line2\n",
        "file_to_split.2.batch": "\n\n",
        "file_to_split.3.batch": "line3\n",
    }
    input_path = join_paths("input", "file_to_split")
    strategy = SplitToSize(FileSize(6))
    split_file(input_path, temp_dir, strategy)
    assert_has_content(temp_dir, expected_content)


def test_sort_single_file(temp_dir):
    input_path = join_paths("input", "files_to_sort", "file1")
    output_path = join_paths(temp_dir, "sorted")
    sort_single_file(input_path, output_path)
    expected_text = "\n 2\n1\n2\n3\n"
    assert read(output_path) == expected_text


def test_sort_single_file_to_dir(temp_dir):
    input_path = join_paths("input", "files_to_sort", "file1")
    output_path = sort_single_file_to_dir(input_path, temp_dir)
    assert output_path == join_paths(temp_dir, "file1")
    expected_text = "\n 2\n1\n2\n3\n"
    assert read(output_path) == expected_text


def test_sort_files_separately(temp_dir):
    input_paths = get_input_paths("files_to_sort")
    sort_files_separately(input_paths, temp_dir)
    expected_content = {"file1": "\n 2\n1\n2\n3\n", "file2": "1\n4\n"}
    assert_has_content(temp_dir, expected_content)


def test_sort_files_together(temp_dir):
    input_paths = get_input_paths("files_to_sort")
    separate_sort_dir = join_paths(temp_dir, "sort")
    sort_files_together(input_paths, temp_dir, separate_sort_dir)
    expected_content = {"0": "\n 2\n1\n", "1": "2\n3\n4\n"}
    assert_has_content(temp_dir, expected_content)
