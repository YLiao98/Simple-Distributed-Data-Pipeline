from assignment_12 import *
# test if using Scan get_next() gets the copy of input

def testPredicate1(input):
    return input[0] == 1

def testPredicate2(input):
    return input[1] == 1

def testAGGR(input):
    return int(sum(input)/len(input))

testScan1 = Scan(filepath="../data/test1.txt")
testScan2 = Scan(filepath="../data/test2.txt")
testSelect1 = Select(input=testScan1, predicate=testPredicate1)
testSelect2 = Select(input = testScan2,predicate=testPredicate2)
testJoin = Join(left_input=testSelect1, right_input=testSelect2,left_join_attribute=1,right_join_attribute=0)
testGroupby = GroupBy(input= testJoin, key=None, value = 4, agg_fun = testAGGR)

outF = open("test.txt","w")
for t in testGroupby.get_next():
    
    line = ' '.join(str(x) for x in t.tuple)
    outF.write(line+"\n" )
outF.close()