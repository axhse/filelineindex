## About

**`filelineindex`** is a tool that enables quick checks to determine if a line is in a predefined line set.

For this purpose, **`filelineindex`** creates an index over the predefined set. The index duplicates the predefined data to speed up searches and uses the file system to store its data. The predefined set is considered immutable and should be specified during index creation.

The indexer splits the predefined set into batches. Therefore, the index requires `[predefined set size] + [max batch size]` of memory to operate.

The index resource directory contains comprehensive data, including the predefined set data, allowing the index to be moved or copied to any computer.

However, additional memory is needed for preprocessing and indexing. When removing input data at each step, having extra memory equivalent to the size of the predefined set is sufficient.


## Usage

To index file lines, prepare the files using the **`preprocess`** module (preprocessing is a mandatory step, primarily involving sorting and ordering file lines). Then, utilize the **`indexer`** module to create an index.

Example
```python
from filelineindex.indexer import Indexer, IndexerOptions
from filelineindex.preprocess import merge_sorted_files, sort_files_separately

# Imagine we have some files to be indexed.
data_files = ["data1.txt", "data2.txt", "data3.txt"]

# Firstly, we should preprocess them (if necessary).

# Sort files separately.
sorted_files = sort_files_separately(data_files, "/tmp/sorted_data")
# Merge sorted files.
preprocessed_files = merge_sorted_files(sorted_files, "/tmp/preprocessed_data")

# Create an indexer.
options = IndexerOptions(min_file_count=10, max_file_count=1000)
indexer = Indexer("/tmp/index_resources", options)

# Index preprocessed files.
index = indexer.index(preprocessed_files)

# Use the index to check if files have specified lines.
has_hello = "hello" in index
has_world = index.has("world")

# If you have already indexed files, you may just load the existing index from its resources.
some_index = Indexer("/tmp/some_index_resources").load_index()
```


## Code Structure

- **abstract:** Specifies common abstract base classes.
- **indexer:** Describes the index builder.
- **preprocess:** Describes tools to prepare files to be indexed.

- **core/algorithm:** Describes common algorithms.
- **core/batched_index:** Describes the line index implemented with batched storage.
- **core/batched_storage:** Describes batched line storage.
- **core/filetools:** Describes auxiliary file tools for other modules.
