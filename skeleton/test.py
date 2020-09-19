from assignment_12 import *
# test if using Scan get_next() gets the copy of input
testScan = Scan(filepath="../data/friends.txt")
testSelect = Select(input=testScan, user_attr=0,value = 1)

outF = open("test.txt","w")
for t in testSelect.get_next():
    
    line = ' '.join(str(x) for x in t.tuple)
    outF.write(line+"\n" )
outF.close()