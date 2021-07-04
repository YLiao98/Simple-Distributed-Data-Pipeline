from assignment_12_ray import *
import pytest
import filecmp
import pdb

ray.init()
#simple test to test scan operator, compare the content with source file
def testScan0():
    testScan = Scan.remote(filepath="../data/test1.txt")
    outF = open("testScan.txt","w")
    while True:
        batch = ray.get(testScan.get_next.remote())
        if batch == None: break
        for t in batch:
            line = ' '.join(x for x in t.tuple)
            outF.write(line+"\n" )
    outF.close()
    assert filecmp.cmp("testScan.txt", "../data/test1.txt",shallow=False), "Scan operation failed, incorrect scan"

#filtered method passed in, and test if the length returned from Scan matches the size in the source file
def testScan1():
    #define a predicate(filter) function
    def testPredicate(input):
        return input[0] == "7"

    testScan = Scan.remote(filepath="../data/test1.txt",filter=testPredicate)
    total_batch = []
    with open("../data/test1.txt","r",newline = '') as f:
                reader = csv.reader(f,delimiter=' ')
                lst = list(reader)
    f.close()
    filtered_lst = list(filter(lambda x: x[0] == "7", lst))
    while True:
        batch = ray.get(testScan.get_next.remote())
        if batch == None: break
        for t in batch:
            total_batch.append(t)
    assert len(filtered_lst) == len(total_batch), "incorrect scanned size with predicate function passed in"

#filtered method passed in, and test if the content returned from Scan matches the ones in the source file
def testScan2():
    #define a predicate(filter) function
    def testPredicate(input):
        return input[0] == "7"

    testScan = Scan.remote(filepath="../data/test1.txt",filter=testPredicate)
    ifSame = True
    total_batch = []
    #retrieve source file and get filtered content
    with open("../data/test1.txt","r",newline = '') as f:
                reader = csv.reader(f,delimiter=' ')
                lst = list(reader)
    f.close()
    filtered_lst = list(filter(lambda x: x[0] == "7", lst))
    while True:
        batch = ray.get(testScan.get_next.remote())
        if batch == None: break
        for t in batch:
            total_batch.append(t)
    for i in range(0, len(total_batch)):
        if total_batch[i].tuple[0] != filtered_lst[i][0]:
            ifSame = False
        
    assert ifSame, "Scan filter content is not the same."


#filtered method passed in, and test if the content returned from Select matches the ones in the source file
def testSelect():
    #define a predicate(filter) function
    def testPredicate(input):
        return input[0] == "7"
    #initial 
    testScan = Scan.remote(filepath="../data/test1.txt")
    testSelect = Select.remote(input=testScan,predicate = testPredicate)
    ifSame = True
    total_batch = []
    #retrieve source file and get filtered content
    with open("../data/test1.txt","r",newline = '') as f:
                reader = csv.reader(f,delimiter=' ')
                lst = list(reader)
    f.close()
    filtered_lst = list(filter(lambda x: x[0] == "7", lst))
    # get filtered output from Select operator
    while True:
        batch = ray.get(testSelect.get_next.remote())
        if batch == None: break
        for t in batch:
            total_batch.append(t)
    for i in range(0, len(total_batch)):
        if total_batch[i].tuple[0] != filtered_lst[i][0]:
            ifSame = False
        
    assert ifSame, "Select filter content is not the same."

#test Project operator, compare the content from Project.get_next() to expected content file
def testProject():
    testScan = Scan.remote(filepath="../data/test1.txt")
    testProject = Project.remote(input = testScan, fields_to_keep=[0])
    outF = open("testProject.txt","w")
    while True:
        batch = ray.get(testProject.get_next.remote())
        if batch == None: break
        for t in batch:
            line = ' '.join(x for x in t.tuple)
            outF.write(line+"\n" )
    outF.close()
    assert filecmp.cmp("testProject.txt", "../data/test1_project.txt",shallow=False), "Project operation failed, incorrect selection on column"

#test Join operator, equi join on test1.uid2 = test2.uid, test1.uid1 = '5'
def testJoin2():
    def predicate(input):
        return input[0] == "5"
    testScan = Scan.remote(filepath="../data/test1.txt",filter = predicate)
    testScan1 = Scan.remote(filepath="../data/test2.txt")
    testJoin = Join.remote(left_input=testScan, right_input=testScan1, left_join_attribute=0,right_join_attribute=0)
    outF = open("pytest_join2.txt","w")
    while True:
        batch = ray.get(testJoin.get_next.remote())
        if batch == None: break
        for t in batch:
            line = ' '.join(x for x in t.tuple)
            outF.write(line+"\n" )
    outF.close()
    assert filecmp.cmp("pytest_join2.txt", "../data/pytestJoin2.txt",shallow=False), "Join operation failed, incorrect content on Join output"


#test Join operator, equi join on test1.uid2 = test2.uid, test1.uid1 = '5'
def testJoin():
    def predicate(input):
        return input[0] == "5"
    testScan = Scan.remote(filepath="../data/test1.txt",filter = predicate)
    testScan1 = Scan.remote(filepath="../data/test2.txt")
    testJoin = Join.remote(left_input=testScan, right_input=testScan1, left_join_attribute=1,right_join_attribute=0)
    outF = open("pytest_join.txt","w")
    while True:
        batch = ray.get(testJoin.get_next.remote())
        if batch == None: break
        for t in batch:
            line = ' '.join(x for x in t.tuple)
            outF.write(line+"\n" )
    outF.close()
    assert filecmp.cmp("pytest_join.txt", "../data/pytestJoin.txt",shallow=False), "Join operation failed, incorrect content on Join output"

#groupby toy movie rating file, groupby movie id
def testGroupby2():
    def testAgg(input):
        return round(sum(input)/len(input), 1)
    testScan = Scan.remote(filepath="../data/test2.txt")
    testGroupby2 = GroupBy.remote(input=testScan,key = 1, value=2,agg_fun=testAgg)
    outF = open("testGroupby.txt","w")
    while True:
        batch = ray.get(testGroupby2.get_next.remote())
        if batch == None: break
        for t in batch:    
            line = ' '.join(str(x) for x in t.tuple)
            outF.write(line+"\n" )
    outF.close()
    assert filecmp.cmp("testGroupby.txt", "../data/pytestGroupby.txt",shallow=False), "Groupby operation failed, incorrect content"

#test Groupby operator, in this test, we test without grouping, only aggregating
def testGroupby1():
    def testAgg(input):
        return round(sum(input)/len(input),1)
    testScan = Scan.remote(filepath="../data/test2.txt")
    testGroupby = GroupBy.remote(input=testScan,key = None, value=2,agg_fun=testAgg)
    while True:
        batch = ray.get(testGroupby.get_next.remote())
        if batch == None: break
        aggregated_val=batch[0].tuple[0]
    assert aggregated_val == "3.6", "Groupby operation failed, incorrect value on aggregated result"

#test OrderBy operator, in this test, we test with reversing
def testOrderBy1():

    testScan = Scan.remote(filepath="../data/testSort.txt")
    testOrderby = OrderBy.remote(input=testScan,comparator=lambda x : x.tuple[1], ASC=False)
    outF = open("testOrderby.txt","w")
    while True:
        batch = ray.get(testOrderby.get_next.remote())
        if batch == None: break
        for t in batch:    
            line = ' '.join(str(x) for x in t.tuple)
            outF.write(line+"\n" )
    outF.close()
    assert filecmp.cmp("testOrderby.txt", "../data/testSort.txt",shallow=False), "OrderBy operation failed, incorrect content"

#test OrderBy operator, in this test, we test without reversing
def testOrderBy2():

    testScan = Scan.remote(filepath="../data/testSort.txt")
    testOrderby = OrderBy.remote(input=testScan,comparator=lambda x : x.tuple[0], ASC=True)
    outF = open("testOrderby2.txt","w")
    while True:
        batch = ray.get(testOrderby.get_next.remote())
        if batch == None: break
        for t in batch:    
            line = ' '.join(str(x) for x in t.tuple)
            outF.write(line+"\n" )
    outF.close()
    assert filecmp.cmp("testOrderby2.txt", "../data/testSort.txt",shallow=False), "Orderby operation failed, incorrect content"

#test top K operator, k = 3
def testTopK():

    testScan = Scan.remote(filepath="../data/test1.txt")
    testHisto = Histogram.remote(input=testScan,key=0)
    outF = open("testHisto.txt","w")
    while True:
        batch = ray.get(testHisto.get_next.remote())
        if batch == None: break
        for t in batch:    
            line = ' '.join(str(x) for x in t.tuple)
            outF.write(line+"\n" )
    outF.close()
    assert filecmp.cmp("testHisto.txt", "../data/pytestHisto.txt",shallow=False), "Histo operation failed, incorrect content"

#test task 1 function, and compare with the expected result
def testTask1():
    def predicate(input):
        return input[0] == "3"
    def predicate1(input):
        return input[1] == "2"
    def testAgg(input):
        return round(sum(input)/len(input),1)
    testScan1 = Scan.remote(filepath="../data/test1.txt",filter = predicate)
    testScan2 = Scan.remote(filepath="../data/test2.txt",filter = predicate1)
    testJoin = Join.remote(left_input=testScan1,right_input=testScan2,left_join_attribute=1,right_join_attribute=0)
    testGroupby = GroupBy.remote(input = testJoin,key=None,value=4,agg_fun=testAgg)
    while True:
        batch = ray.get(testGroupby.get_next.remote())
        if batch == None: break
        aggregated_val=batch[0].tuple[0]
    assert aggregated_val == "3.2", "task 1 operation failed, incorrect value on aggregated result"

    
#test task 2 function, and compare with the expected result
def testTask2():
    def predicate(input):
        return input[0] == "7"
    def testAgg(input):
        return round(sum(input)/len(input),1)
    testScan1 = Scan.remote(filepath="../data/test1.txt",filter = predicate)
    testScan2 = Scan.remote(filepath="../data/test2.txt")
    testJoin = Join.remote(left_input=testScan1,right_input=testScan2,left_join_attribute=1,right_join_attribute=0)
    testGroupby = GroupBy.remote(input = testJoin,key=3,value=4,agg_fun=testAgg)
    testOrderby = OrderBy.remote(input = testGroupby,comparator=lambda x:x.tuple[1],ASC=False)
    testTopK = TopK.remote(input=testOrderby,k = 1)
    testProject = Project.remote(input=testTopK,fields_to_keep=[0])
    while True:
        batch = ray.get(testProject.get_next.remote())
        if batch == None: break
        output=batch[0].tuple[0]
    assert output == "1", "task 2 operation failed, incorrect value."

#test task 2 function, and compare with the expected result
def testTask3():
    def predicate(input):
        return input[0] == "6"
    def predicate1(input):
        return input[1] == "2"
    testScan1 = Scan.remote(filepath="../data/test1.txt",filter = predicate)
    testScan2 = Scan.remote(filepath="../data/test2.txt",filter=predicate1)
    testJoin = Join.remote(left_input=testScan1,right_input=testScan2,left_join_attribute=1,right_join_attribute=0)
    testHisto = Histogram.remote(input=testJoin,key=4)
    testOrderby = OrderBy.remote(input=testHisto,comparator= lambda x: x.tuple[0],ASC=True)
    outF = open("testTask3.txt","w")
    while True:
        batch = ray.get(testOrderby.get_next.remote())
        if batch == None: break
        for t in batch:    
            line = ' '.join(str(x) for x in t.tuple)
            outF.write(line+"\n" )
    outF.close()
    assert filecmp.cmp("testTask3.txt", "../data/pytestTask3.txt",shallow=False), "Orderby operation failed, incorrect content"
