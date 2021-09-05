# Query Processing Algorithms and Data-Provenance-Tracking

## Pre-requisites for running queries:

1. Python (3.7+)
2. [Pytest](https://docs.pytest.org/en/stable/)
3. [Ray](https://ray.io)

## Input Data

Queries of assignments 1 and 2 expect two space-delimited text files (similar to CSV files). 

The first file (friends) must include records of the form:

|UID1 (int)|UID2 (int)|
|----|----|
|1   |2342|
|231 |3   |
|... |... |

The second file (ratings) must include records of the form:

|UID (int)|MID (int)|RATING (int)|
|---|---|------|
|1  |10 |4     |
|231|54 |2     |
|...|...|...   |

## Running queries of Assignment 1

You can run queries as shown below: 

```bash
$ python assignment_12.py --task [task_number] --friends [path_to_friends_file.txt] --ratings [path_to_ratings_file.txt] --uid [user_id] --mid [movie_id]
```

For example, the following command runs the 'likeness prediction' query of the first task for user id 10 and movie id 3:

```bash
$ python assignment_12.py --task 1 --friends friends.txt --ratings ratings.txt --uid 10 --mid 3
```
Note that movie id is not needed in task 2. The 'recommendation' query of the second task does not require a movie id. If you provide a `--mid` argument, it will be simply ignored.

## Running queries of Assignment 2

### Task 4 Retrieve the lineage of Task 2
Run the task as shown below

```bash
$ python assignment_12.py --task 4 --friends [path_to_friends_file.txt] --ratings [path_to_ratings_file.txt] --uid [user_id] 
```

### Task 5 Retrieve the where-provenance of Task 1 
Run the task as shown below

```bash
$ python assignment_12.py --task 5 --friends [path_to_friends_file.txt] --ratings [path_to_ratings_file.txt] --uid [user_id] --mid [movie_id]
```

### Task 6 Retrieve the How_provenance of Query in Task 2
Run the task as shown below

```bash
$ python assignment_12.py --task 6 --friends [path_to_friends_file.txt] --ratings [path_to_ratings_file.txt] --uid [user_id] 
```

### Task 7 Retrieve the Responsibility with p >= 0.5 of lineage tuples in Task 2
Run the task as shown below

```bash
$ python assignment_12.py --task 7 --friends [path_to_friends_file.txt] --ratings [path_to_ratings_file.txt] --uid [user_id] 
```
