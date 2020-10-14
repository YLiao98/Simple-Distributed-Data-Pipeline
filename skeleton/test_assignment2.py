from assignment_12 import *
import pytest
import filecmp
import pdb

'''
def testScan0_lineage():
    testScan = Scan(filepath="../data/test1.txt")
    batch = testScan.get_next() #first batch
    first_tuple = batch[0] #first tuple in the batch
    second_tuple = batch[1]
    lst = first_tuple.lineage()
    atuple1 = lst[0] #should be only one
    lst2 = second_tuple.lineage()
    assert str(atuple1.tuple) == "('1', '2')" and str(lst2[0].tuple) == "('1', '3')" 

def testProject_lineage():
    testScan = Scan(filepath="../data/test1.txt")
    testProject = Project(input=testScan,fields_to_keep=[1],track_prov=True)
    batch = testProject.get_next() #first batch
    first_tuple = batch[0]
    print(testProject.cache_mapping[(0,tuple('2'))].tuple)
    print(testProject.input.cached)
    lst = first_tuple.lineage()
    assert len(lst) == 1 and str(lst[0].tuple) == "('1', '2')"

def testTopK_lineage():
    testScan = Scan(filepath="../data/test1.txt")
    testProject = Project(input=testScan,fields_to_keep=[1],track_prov=True)
    testOrderby = OrderBy(input=testProject,comparator=lambda x : x.tuple[0],ASC=False,track_prov=True)
    testTopK = TopK(input=testOrderby,k=2,track_prov=True)
    batch = testTopK.get_next() #first batch
    first_tuple=batch[0]
    lst = first_tuple.lineage()
    assert lst[0].tuple == ('2','7')

def testJoin_lineage():
    def predicate(input):
        return input[0] == "5"
    testScan = Scan(filepath="../data/test1.txt",filter = predicate,track_prov=True)
    testScan1 = Scan(filepath="../data/test2.txt",track_prov=True)
    testJoin = Join(left_input=testScan, right_input=testScan1, left_join_attribute=1,right_join_attribute=0,track_prov= True)
    batch = testJoin.get_next()
    first_tuple = batch[0]
    lst = first_tuple.lineage()
    res  = []
    for each in lst:
        res.append(each.tuple)
    assert res == [('5','2'),('2','1','5')]
'''
def testGroupby_avg_lineage():
    def predicate(input):
        return input[0] == "5"
    def testAgg(input):
        return round(sum(input)/len(input), 1)
    testScan = Scan(filepath="../data/test1.txt",filter = predicate,track_prov=True)
    testScan1 = Scan(filepath="../data/test2.txt",track_prov=True)
    testJoin = Join(left_input=testScan, right_input=testScan1, left_join_attribute=1,right_join_attribute=0,track_prov= True)
    testGrouby =GroupBy(input=testJoin,key = None, value = 4,agg_fun=testAgg,track_prov=True)
    batch = testGrouby.get_next()
    lst = batch[0].lineage()
    res = []
    for each in lst:
        res.append(each.tuple)
    assert len(res) == 8 and res == [('5','2'),('2','1','5'),('2','2','4'),('2','4','3'),('5','4'),('4','1','4'),('4','2','3'),('4','3','1')]

def testGroupby_key_lineage():
    def predicate(input):
        return input[0] == "5"
    def testAgg(input):
        return round(sum(input)/len(input), 1)
    testScan = Scan(filepath="../data/test1.txt",filter = predicate,track_prov=True)
    testScan1 = Scan(filepath="../data/test2.txt",track_prov=True)
    testJoin = Join(left_input=testScan, right_input=testScan1, left_join_attribute=1,right_join_attribute=0,track_prov= True)
    testGrouby =GroupBy(input=testJoin,key = 2, value = 4,agg_fun=testAgg,track_prov=True)
    batch = testGrouby.get_next()
    lst = batch[0].lineage()
    res = []
    for each in lst:
        res.append(each.tuple)
    assert len(res) == 4 and res == [('5','2'),('2','1','5'),('2','2','4'),('2','4','3')]