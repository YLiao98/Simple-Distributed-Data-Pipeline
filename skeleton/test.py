from assignment_12 import *
import pytest
import filecmp
import pdb

#simple test to test scan operator, compare the content with source file
def testScan0():
    testScan = Scan(filepath="../data/test1.txt")
    outF = open("testScan.txt","w")
    while True:
        batch = testScan.get_next()
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

    testScan = Scan(filepath="../data/test1.txt",filter=testPredicate)
    total_batch = []
    with open("../data/test1.txt","r",newline = '') as f:
                reader = csv.reader(f,delimiter=' ')
                lst = list(reader)
    f.close()
    filtered_lst = list(filter(lambda x: x[0] == "7", lst))
    while True:
        batch = testScan.get_next()
        if batch == None: break
        for t in batch:
            total_batch.append(t)
    assert len(filtered_lst) == len(total_batch), "incorrect scanned size with predicate function passed in"

#filtered method passed in, and test if the content returned from Scan matches the ones in the source file
def testScan2():
    #define a predicate(filter) function
    def testPredicate(input):
        return input[0] == "7"

    testScan = Scan(filepath="../data/test1.txt",filter=testPredicate)
    ifSame = True
    total_batch = []
    #retrieve source file and get filtered content
    with open("../data/test1.txt","r",newline = '') as f:
                reader = csv.reader(f,delimiter=' ')
                lst = list(reader)
    f.close()
    filtered_lst = list(filter(lambda x: x[0] == "7", lst))
    while True:
        batch = testScan.get_next()
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
    testScan = Scan(filepath="../data/test1.txt")
    testSelect = Select(input=testScan,predicate = testPredicate)
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
        batch = testSelect.get_next()
        if batch == None: break
        for t in batch:
            total_batch.append(t)
    for i in range(0, len(total_batch)):
        if total_batch[i].tuple[0] != filtered_lst[i][0]:
            ifSame = False
        
    assert ifSame, "Select filter content is not the same."

#test Project operator, compare the content from Project.get_next() to expected content file
def testProject():
    testScan = Scan(filepath="../data/test1.txt")
    testProject = Project(input = testScan, fields_to_keep=[0])
    outF = open("testProject.txt","w")
    while True:
        batch = testProject.get_next()
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
    testScan = Scan(filepath="../data/test1.txt",filter = predicate)
    testScan1 = Scan(filepath="../data/test2.txt")
    testJoin = Join(left_input=testScan, right_input=testScan1, left_join_attribute=0,right_join_attribute=0)
    outF = open("pytest_join2.txt","w")
    while True:
        batch = testJoin.get_next()
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
    testScan = Scan(filepath="../data/test1.txt",filter = predicate)
    testScan1 = Scan(filepath="../data/test2.txt")
    testJoin = Join(left_input=testScan, right_input=testScan1, left_join_attribute=1,right_join_attribute=0)
    outF = open("pytest_join.txt","w")
    while True:
        batch = testJoin.get_next()
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
    testScan = Scan(filepath="../data/test2.txt")
    testGroupby2 = GroupBy(input=testScan,key = 1, value=2,agg_fun=testAgg)
    outF = open("testGroupby.txt","w")
    while True:
        batch = testGroupby2.get_next()
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
    testScan = Scan(filepath="../data/test2.txt")
    testGroupby = GroupBy(input=testScan,key = None, value=2,agg_fun=testAgg)
    while True:
        batch = testGroupby.get_next()
        if batch == None: break
        aggregated_val=batch[0].tuple[0]
    assert aggregated_val == "3.6", "Groupby operation failed, incorrect value on aggregated result"

#test OrderBy operator, in this test, we test with reversing
def testOrderBy1():

    testScan = Scan(filepath="../data/testSort.txt")
    testOrderby = OrderBy(input=testScan,comparator=lambda x : x.tuple[1], ASC=False)
    outF = open("testOrderby.txt","w")
    while True:
        batch = testOrderby.get_next()
        if batch == None: break
        for t in batch:    
            line = ' '.join(str(x) for x in t.tuple)
            outF.write(line+"\n" )
    outF.close()
    assert filecmp.cmp("testOrderby.txt", "../data/testSort.txt",shallow=False), "OrderBy operation failed, incorrect content"

#test OrderBy operator, in this test, we test without reversing
def testOrderBy2():

    testScan = Scan(filepath="../data/testSort.txt")
    testOrderby = OrderBy(input=testScan,comparator=lambda x : x.tuple[0], ASC=True)
    outF = open("testOrderby2.txt","w")
    while True:
        batch = testOrderby.get_next()
        if batch == None: break
        for t in batch:    
            line = ' '.join(str(x) for x in t.tuple)
            outF.write(line+"\n" )
    outF.close()
    assert filecmp.cmp("testOrderby2.txt", "../data/testSort.txt",shallow=False), "Orderby operation failed, incorrect content"

#test top K operator, k = 3
def testTopK():

    testScan = Scan(filepath="../data/test1.txt")
    testHisto = Histogram(input=testScan,key=0)
    outF = open("testHisto.txt","w")
    while True:
        batch = testHisto.get_next()
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
    testScan1 = Scan(filepath="../data/test1.txt",filter = predicate)
    testScan2 = Scan(filepath="../data/test2.txt",filter = predicate1)
    testJoin = Join(left_input=testScan1,right_input=testScan2,left_join_attribute=1,right_join_attribute=0)
    testGroupby = GroupBy(input = testJoin,key=None,value=4,agg_fun=testAgg)
    while True:
        batch = testGroupby.get_next()
        if batch == None: break
        aggregated_val=batch[0].tuple[0]
    assert aggregated_val == "3.2", "task 1 operation failed, incorrect value on aggregated result"

    
#test task 2 function, and compare with the expected result
def testTask2():
    def predicate(input):
        return input[0] == "7"
    def testAgg(input):
        return round(sum(input)/len(input),1)
    testScan1 = Scan(filepath="../data/test1.txt",filter = predicate)
    testScan2 = Scan(filepath="../data/test2.txt")
    testJoin = Join(left_input=testScan1,right_input=testScan2,left_join_attribute=1,right_join_attribute=0)
    testGroupby = GroupBy(input = testJoin,key=3,value=4,agg_fun=testAgg)
    testOrderby = OrderBy(input = testGroupby,comparator=lambda x:x.tuple[1],ASC=False)
    testTopK = TopK(input=testOrderby,k = 1)
    testProject = Project(input=testTopK,fields_to_keep=[0])
    while True:
        batch = testProject.get_next()
        if batch == None: break
        output=batch[0].tuple[0]
    assert output == "1", "task 2 operation failed, incorrect value."

#test task 2 function, and compare with the expected result
def testTask3():
    def predicate(input):
        return input[0] == "6"
    def predicate1(input):
        return input[1] == "2"
    testScan1 = Scan(filepath="../data/test1.txt",filter = predicate)
    testScan2 = Scan(filepath="../data/test2.txt",filter=predicate1)
    testJoin = Join(left_input=testScan1,right_input=testScan2,left_join_attribute=1,right_join_attribute=0)
    testHisto = Histogram(input=testJoin,key=4)
    testOrderby = OrderBy(input=testHisto,comparator= lambda x: x.tuple[0],ASC=True)
    outF = open("testTask3.txt","w")
    while True:
        batch = testOrderby.get_next()
        if batch == None: break
        for t in batch:    
            line = ' '.join(str(x) for x in t.tuple)
            outF.write(line+"\n" )
    outF.close()
    assert filecmp.cmp("testTask3.txt", "../data/pytestTask3.txt",shallow=False), "Orderby operation failed, incorrect content"
