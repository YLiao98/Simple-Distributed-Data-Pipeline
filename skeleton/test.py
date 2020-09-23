from assignment_12 import *
import time

# test the time it takes if we scan every tuple all at once

def testPredicate1(input):
    return input[0] == 1710

def testPredicate2(input):
    return input[1] == 99

def testAGGR(input):
    return int(sum(input)/len(input))
start_time = time.time()
testScan1 = Scan(filepath="../data/friends.txt")
testScan2 = Scan(filepath="../data/movie_ratings.txt")
testSelect1 = Select(input=testScan1, predicate=testPredicate1)
testSelect2 = Select(input = testScan2,predicate=testPredicate2)
testJoin = Join(left_input=testSelect1, right_input=testSelect2,left_join_attribute=1,right_join_attribute=0)

testGroupby = GroupBy(input= testJoin, key=None, value = 4, agg_fun = testAGGR)

outF = open("test.txt","w")
for t in testGroupby.get_next():
    
    line = ' '.join(str(x) for x in t.tuple)
    outF.write(line+"\n" )
outF.close()

print("----%s seconds ---" % (time.time()-start_time))