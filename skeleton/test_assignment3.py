import pytest
import filecmp
import sklearn
import math
from assignment_12 import *
from assignment3 import *

# ../click_ad_data/train.csv

# for the simple test dataset, set the variable s value in class Scan __get_file() to 7

def testMissing():
    testScan = Scan(filepath="../data/testAssignmentTask1.csv")

    testProject = Project(input=testScan,fields_to_keep=[8]) # testing the missing value on the devid attribute

    batch = testProject.get_next()
    assert testProject.getCountMissing() == 3

def testDistinct():
    testScan = Scan(filepath="../data/testAssignmentTask1.csv")
    testProject = Project(input = testScan,fields_to_keep=[7])
    testDistinct = Distinct(input = testProject,attr_to_distinct=0) # we want to test distinct browserid, nan is also considered a type
    batch = testDistinct.get_next()
    res=[]

    distinct_count = testDistinct.getSetSize()
    res.append(tuple(['browserid',distinct_count]))
    assert res == [('browserid',4)]
# test distinct values with values that are NaN
def testDistinctWithMissing():
    testScan = Scan(filepath="../click_ad_data/testAssignmentTask1.csv")
    testProject = Project(input = testScan,fields_to_keep=[8])
    testDistinct = Distinct(input = testProject,attr_to_distinct=0) # we want to test distinct dev, nan is also considered a type
    batch = testDistinct.get_next()
    res=[]

    distinct_count = testDistinct.getSetSize()
    res.append(tuple(['browserid',distinct_count]))
    assert res == [('browserid',3)] # we count nan in

# test simple ETL operation
def testMapping():
    testScan = Scan(filepath="../click_ad_data/testAssignmentTask1.csv")
    testMap = Map(input = testScan)
    batch = testMap.get_next()
    # test that missing value, devid and browserid are mapped to a hashed value or zero and there are total of 12 fields for each tuple
    assert len(batch[0].tuple) == 12 and batch[0].tuple[10] == 0 and isinstance(batch[0].tuple[9],int) and isinstance(batch[0].tuple[8],int)
'''

testScan = Scan(filepath="../click_ad_data/train.csv")

testMap = Map(input = testScan,map_func =custom_mapping)
cleaned_lst = []
while True:
    batch = testMap.get_next()
    if batch == None:
        break
    cleaned_lst += batch

print(cleaned_lst[1])




n = sum(1 for line in open("../data/testAssignmentTask1.csv"))-1
# desired sample size
s = 5
skip = sorted(random.sample(range(1,n+1),n-s)) # the 0-indexed header will not be included
df = pandas.read_csv("../data/testAssignmentTask1.csv",skiprows=skip,sep=",")


print(df)
res = list(df.values.tolist())
print(res)
print(math.isnan(float(res[0][8])))
'''
