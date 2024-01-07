## Objective

#### Problem
There are some files, and it's necessary to regularly check whether specific lines exist in any of these files.

#### Restrictions

1. The files have a large total size.
2. The files change rarely.

#### Important Metrics

1. Performance - how fast the checks are performed.
2. Memory-effectiveness - how much memory is used.


## Solution Variations

#### 1. Database
A single table is used to store the lines as a primary key column.

#### 2. In-memory Array
The lines are stored as a sorted in-memory array.

#### 3. Batched Files
The lines are stored in a batched way with files, full batches are loaded to perform binary search.

#### 4. Seekable Search
Same as Batched Files, but file.seek() is used to perform binary search.

|                                  | Database | In-memory array | Batched files | Seekable Search |
|----------------------------------|----------|-----------------|---------------|-----------------|
| High check performance           | ❓        | ❓               | ❓             | ❓               |
| High update performance          | ✔️       | ✔️              | ❌             | ❌               |
| Usage memory-efficiency          | ❓        | ❌               | ✔️            | ✔️              |
| Initialization memory-efficiency | ❓        | ❓               | ❓             | ❓               |
