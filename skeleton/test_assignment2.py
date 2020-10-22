import pytest
import filecmp
import pdb
from assignment_12 import *
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

def testTask1Example():
    def predicate(input):
        return input[0] == "0"
    def testAgg(input):
        return round(sum(input)/len(input),1)
    testScan1 = Scan(filepath="../data/text3.txt",filter = predicate,track_prov=True)
    testScan2 = Scan(filepath="../data/text4.txt",track_prov=True)
    testJoin = Join(left_input=testScan1,right_input=testScan2,left_join_attribute=1,right_join_attribute=0,track_prov=True)
    testGroupby = GroupBy(input = testJoin,key=3,value=4,agg_fun=testAgg,track_prov=True)
    testOrderby = OrderBy(input = testGroupby,comparator=lambda x:x.tuple[1],ASC=False,track_prov=True)
    testTopK = TopK(input=testOrderby,k = 1,track_prov=True)
    testProject = Project(input=testTopK,fields_to_keep=[0],track_prov=True)
    res = []
    lin = []
    while True:
        batch = testProject.get_next()
        if batch == None: break
        res += batch
    logger.debug(res)
    first_tuple = res[0]
    lst = first_tuple.lineage()
    for each in lst:
        lin.append(each.tuple)
    assert len(res)==1 and lin ==[('0','1'),('1','10','5'),('0','4'),('4','10','8'),('0','18'),('18','10','2')]

def testWhere_scan():
    testScan1 = Scan(filepath="../data/test1.txt",track_prov=True)
    batch = testScan1.get_next() #first batch
    first_tuple = batch[13] #first tuple in the batch
    first_tuple_where=first_tuple.where(1)
    assert str(first_tuple) == "('4', '5')" and str(first_tuple_where) == "[('../data/test1.txt', 14, ('4', '5'), '5')]"

def testWhere_project():
    testScan1 = Scan(filepath="../data/test2.txt",track_prov=True)
    testProject = Project(input=testScan1,fields_to_keep=[0,2],track_prov=True)
    batch = testProject.get_next() #first batch
    test_tuple = batch[13] #first tuple in the batch
    test_tuple_where=test_tuple.where(1)
    assert str(test_tuple) == "('5', '4')" and str(test_tuple_where) == "[('../data/test2.txt', 14, ('5', '1', '4'), '4')]"
 
def testWhere_join():
    def predicate(input):
            return input[0] == "5"
    testScan = Scan(filepath="../data/test1.txt",filter = predicate,track_prov=True)
    testScan1 = Scan(filepath="../data/test2.txt",track_prov=True)
    testJoin = Join(left_input=testScan, right_input=testScan1, left_join_attribute=1,right_join_attribute=0,track_prov= True)
    batch = testJoin.get_next()
    test_tuple = batch[4]
    test_tuple_where = test_tuple.where(3)
    assert str(test_tuple) == "('5', '4', '4', '2', '3')" and str(test_tuple_where) == "[('../data/test2.txt', 9, ('4', '2', '3'), '2')]"   

def testWhere_groupby_avg():
    def predicate(input):
        return input[0] == "5"
    def testAgg(input):
        return round(sum(input)/len(input), 1)
    testScan = Scan(filepath="../data/test1.txt",filter = predicate,track_prov=True)
    testScan1 = Scan(filepath="../data/test2.txt",track_prov=True)
    testJoin = Join(left_input=testScan, right_input=testScan1, left_join_attribute=1,right_join_attribute=0,track_prov= True)
    testGrouby =GroupBy(input=testJoin,key = None, value = 4,agg_fun=testAgg,track_prov=True)
    batch = testGrouby.get_next()
    lst = batch[0].where(0)
    assert len(batch) == 1 and str(lst)=="[('../data/test2.txt', 5, ('2', '1', '5'), '5'), ('../data/test2.txt', 6, ('2', '2', '4'), '4'), ('../data/test2.txt', 7, ('2', '4', '3'), '3'), ('../data/test2.txt', 8, ('4', '1', '4'), '4'), ('../data/test2.txt', 9, ('4', '2', '3'), '3'), ('../data/test2.txt', 10, ('4', '3', '1'), '1')]"

def testWhere_groupby_key():
    def predicate(input):
        return input[0] == "5"
    def testAgg(input):
        return round(sum(input)/len(input), 1)
    testScan = Scan(filepath="../data/test1.txt",filter = predicate,track_prov=True)
    testScan1 = Scan(filepath="../data/test2.txt",track_prov=True)
    testJoin = Join(left_input=testScan, right_input=testScan1, left_join_attribute=1,right_join_attribute=0,track_prov= True)
    testGrouby =GroupBy(input=testJoin,key = 1, value = 4,agg_fun=testAgg,track_prov=True)
    batch = testGrouby.get_next()
    lst = batch[1].where(1)
    lst2 = batch[0].where(0)
    assert len(batch) == 2 and str(lst) == "[('../data/test2.txt', 8, ('4', '1', '4'), '4'), ('../data/test2.txt', 9, ('4', '2', '3'), '3'), ('../data/test2.txt', 10, ('4', '3', '1'), '1')]" and str(lst2) == "[('../data/test1.txt', 18, ('5', '2'), '2')]"

def testHow_Scan():
    def predicate(input):
        return input[0] == "5"
    testScan = Scan(filepath="../data/friends.txt",filter = predicate,track_prov=True,propagate_prov=True)
    testOutput=[]
    while True:
        batch = testScan.get_next()
        if batch == None: break
        testOutput += batch

    assert testOutput[0].how() == "SCAN((f69219))"

def testHow_Join():
    def predicate(input):
        return input[0] == "5"
    testScan = Scan(filepath="../data/friends.txt",filter = predicate,track_prov=True,propagate_prov=True)
    def predicateHow(input):
        return input[0] == "16"
    testScan2 = Scan(filepath="../data/movie_ratings.txt",filter = predicateHow,track_prov=True,propagate_prov=True)
    testJoin = Join(left_input=testScan,right_input=testScan2,left_join_attribute=1,right_join_attribute=0,track_prov=True,propagate_prov=True)
    testOutput2 = []
    while True:
        batch = testJoin.get_next()
        if batch == None: break
        testOutput2 += batch
    assert testOutput2[0].how() == "JOIN((f69230*r94164))"


def testHow_project():
    def predicate(input):
        return input[0] == "5"
    testScan = Scan(filepath="../data/friends.txt",filter = predicate,track_prov=True,propagate_prov=True)
    testProject = Project(input=testScan,fields_to_keep=[1],propagate_prov=True)
    testOrderby = OrderBy(input=testProject,comparator=lambda x : x.tuple[0],track_prov=True, ASC=False,propagate_prov=True)
    testTopK = TopK(input=testOrderby,k = 4,track_prov=True,propagate_prov=True)
    testOutput=[]
    while True:
        batch = testTopK.get_next()
        if batch == None: break
        testOutput += batch

    assert testOutput[0].how() == "SCAN((f69237))"

def testHow_Orderby():
    def predicate(input):
        return input[0] == "5"
    testScan = Scan(filepath="../data/friends.txt",filter = predicate,track_prov=True,propagate_prov=True)
    testProject = Project(input=testScan,fields_to_keep=[1],propagate_prov=True)
    testOrderby = OrderBy(input=testProject,comparator=lambda x : x.tuple[0],track_prov=True, ASC=False,propagate_prov=True)
    
    testOutput=[]
    while True:
        batch = testOrderby.get_next()
        if batch == None: break
        testOutput += batch

    assert testOutput[0].how() == "SCAN((f69237))"

def testHow_TopK():
    def predicate(input):
        return input[0] == "5"
    testScan = Scan(filepath="../data/friends.txt",filter = predicate,track_prov=True,propagate_prov=True)
    testProject = Project(input=testScan,fields_to_keep=[1],propagate_prov=True)
    testOrderby = OrderBy(input=testProject,comparator=lambda x : x.tuple[0],track_prov=True, ASC=False,propagate_prov=True)
    testTopK = TopK(input=testOrderby,k = 4,track_prov=True,propagate_prov=True)
    testOutput=[]
    while True:
        batch = testTopK.get_next()
        if batch == None: break
        testOutput += batch

    assert testOutput[0].how() == "SCAN((f69237))"

def testHow_AVG():
    def testAgg(input):
        return round(sum(input)/len(input),1)
    def predicate(input):
        return input[0] == "5"
    testScan = Scan(filepath="../data/friends.txt",filter = predicate,track_prov=True,propagate_prov=True)
    testAVG = GroupBy(input=testScan,key=None,value = 1,agg_fun=testAgg,track_prov=False,propagate_prov=True)
    testOutput=[]
    while True:
        batch = testAVG.get_next()
        if batch == None: break
        testOutput += batch
    assert testOutput[0].how() == "AVG((f69219@1608),(f69220@1622),(f69221@1221),(f69222@799),(f69223@1393),(f69224@604),(f69225@1025),(f69226@479),(f69227@170),(f69228@1513),(f69229@749),(f69230@16),(f69231@550),(f69232@1819),(f69233@27),(f69234@468),(f69235@618),(f69236@1423),(f69237@925),(f69238@1675))"

def testHow_Groupby():
    def testAgg(input):
        return round(sum(input)/len(input),1)
    def predicate(input):
        return input[0] == "6" or input[0] == "16"
    testScan = Scan(filepath="../data/movie_ratings.txt",filter = predicate,track_prov=True,propagate_prov=True)
    testAVG = GroupBy(input=testScan,key=1,value = 2,agg_fun=testAgg,track_prov=False,propagate_prov=True)
    testOutput=[]
    while True:
        batch = testAVG.get_next()
        if batch == None: break
        testOutput += batch

    assert testOutput[0].how() == "AVG((r7@4),(r94164@2))"
'''
def testHow_task3():
    def testAgg(input):
        return round(sum(input)/len(input),1)
    def predicate(input):
        return input[0] == "6"
    def predicate3(input):
        return input[0] == "66"
    testScan1 = Scan(filepath="../data/friends.txt",filter = predicate,propagate_prov=True)
    testScan2 = Scan(filepath="../data/movie_ratings.txt",filter=predicate3,propagate_prov = True)
    testJoin = Join(left_input=testScan1,right_input=testScan2,left_join_attribute=1,right_join_attribute=0,propagate_prov=True)
    testGroupby = GroupBy(input = testJoin,key=3,value=4,agg_fun=testAgg,propagate_prov= True)
    testOrderby = OrderBy(input = testGroupby,comparator=lambda x:x.tuple[1],ASC=False,propagate_prov=True)
    testTopK = TopK(input=testOrderby,k = 1,propagate_prov=True)
    testProject = Project(input=testTopK,fields_to_keep=[0],propagate_prov=True)
    testOutput=[]
    while True:
        batch = testProject.get_next()
        if batch == None: break
        testOutput += batch

    assert testOutput[0].how() == "AVG((f15635*r218353@5))"