from assignment_12_block import *
import time
# test if we scan a batch of tuples from a file

def testPredicate1(input):
    return input[0] == 1

def testPredicate2(input):
    return input[1] == 1

def testAGGR(input):
    return int(sum(input)/len(input))

start_time = time.time()
testScan1 = Scan(filepath="../data/friends.txt")
testProject = Project(input=testScan1,fields_to_keep=[0])

outF = open("testProject.txt","w")
while(not testProject.input.end_of_file):
    for t in testProject.get_next():
        
        line = ' '.join(x for x in t.tuple)
        outF.write(line+"\n" )
outF.close()
print("----%s seconds ---" % (time.time()-start_time))

"""
#test select process, from friends select where f.id1 = 1 
start_time = time.time()
testScan1.end_of_file = False
testScan1.curr=0
def test_predicate(input):
    return input[0] == "1"
testSelect = Select(input=testScan1,predicate=test_predicate)

outF = open("testSelect.txt","w")
print(testSelect.input.end_of_file)
while(not testSelect.input.end_of_file):
    for t in testSelect.get_next():
        
        line = ' '.join(x for x in t.tuple)
        outF.write(line+"\n" )
outF.close()

print("----%s seconds ---" % (time.time()-start_time))
"""
"""
# test join process, from friends, movies select * where f.id1 = 1 and m.mid = 1 and f.id1 = m.uid
start_time = time.time()

def test_predicate_2(input):
    return input[1] == "99"

def test_predicate(input):
    return input[0] == "1710"

testScan1 = Scan(filepath="../data/friends.txt")
testScan1.end_of_file = False
testScan1.curr=0
testSelect = Select(input=testScan1,predicate=test_predicate)

testScan2= Scan(filepath="../data/movie_ratings.txt")

testSelect2 = Select(input = testScan2, predicate = test_predicate_2)
testJoin = Join(left_input=testSelect,right_input=testSelect2,left_join_attribute=1,right_join_attribute=0)
outF = open("testJoin.txt","w")
while(not testJoin.end_of_batch):
    batch = testJoin.get_next()
    if batch != None:
        for t in batch:    
            line = ' '.join(x for x in t.tuple)
            outF.write(line+"\n" )
outF.close()

print("----%s seconds ---" % (time.time()-start_time))
"""
"""
# test groupby process, from friends, movies select * where f.id1 = 1 and m.mid = 1 and f.id1 = m.uid
start_time = time.time()

def test_predicate_2(input):
    return input[1] == "99"

def test_predicate(input):
    return input[0] == "1710"
def testAGGR(input):
    return round(sum(input)/len(input))

testScan1 = Scan(filepath="../data/friends.txt")
testScan1.end_of_file = False
testScan1.curr=0
testSelect = Select(input=testScan1,predicate=test_predicate)

testScan2= Scan(filepath="../data/movie_ratings.txt")

testSelect2 = Select(input = testScan2, predicate = test_predicate_2)
testJoin = Join(left_input=testSelect,right_input=testSelect2,left_join_attribute=1,right_join_attribute=0)
testGroupby = GroupBy(input= testJoin, key=None,value = 4,agg_fun=testAGGR)
outF = open("testgroupby.txt","w")
while(not testGroupby.input.end_of_batch):
    batch = testGroupby.get_next()
    if batch != None:
        for t in batch:    
            line = ' '.join(str(x) for x in t.tuple)
            outF.write(line+"\n" )
outF.close()

print("----%s seconds ---" % (time.time()-start_time))
"""