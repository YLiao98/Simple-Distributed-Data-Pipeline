# Assignment 3 Submission

## Pre-requisites for running this assignment

1. Python (3.7+)
2. [Pytest](https://docs.pytest.org/en/stable/)
3. [LightGBM](https://github.com/microsoft/LightGBM)
4. [LIME](https://github.com/marcotcr/lime)
5. [SHAP](https://github.com/slundberg/shap)
6. [pandas](https://pypi.org/project/pandas/)


## Running tasks of assignment

You can run task as shown below: 

```bash
$ python assignment3.py --task [task_number] --data [path_to_dataset]
```
For example, the following command runs the fourth task 

```bash
$ python assignment3.py --task 3 --data "../click_ad_data/train.csv"
```

Notice that *--task 3*    runs the fourth task. 

### Key Information

1. Data Preprocessing and Scanning the dataset could take a long time (10~30 seconds)
2. The Task 2 ETL operations in assignment description does not require printing results, therefore this operation is implemeted but showing, we can only see results from running Task 3 and 4
3. Task1 is defined in runTask1(), Task3 is defined in runTask2(), Task4 is defined in runTask3()
