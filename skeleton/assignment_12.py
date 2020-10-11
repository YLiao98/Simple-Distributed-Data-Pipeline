from __future__ import absolute_import
from __future__ import annotations
from __future__ import division
from __future__ import print_function
# aside from assignment default import, import argparse to build CLI
import csv
import logging
from typing import List, Tuple
import uuid
import argparse
from argparse import RawTextHelpFormatter 
import ray

# Note (john): Make sure you use Python's logger to log
#              information about your program
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

# Generates unique operator IDs
def _generate_uuid():
    return uuid.uuid4()


# Custom tuple class with optional metadata
class ATuple:
    """Custom tuple.

    Attributes:
        tuple (Tuple): The actual tuple.
        metadata (string): The tuple metadata (e.g. provenance annotations).
        operator (Operator): A handle to the operator that produced the tuple.
    """
    def __init__(self, tuple, metadata=None, operator=None):
        self.tuple = tuple
        self.metadata = metadata
        self.operator = operator

    # Returns the lineage of self
    def lineage() -> List[ATuple]:
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the Where-provenance of the attribute at index 'att_index' of self
    def where(att_index) -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Returns the How-provenance of self
    def how() -> string:
        # YOUR CODE HERE (ONLY FOR TASK 3 IN ASSIGNMENT 2)
        pass

    # Returns the input tuples with responsibility \rho >= 0.5 (if any)
    def responsible_inputs() -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 4 IN ASSIGNMENT 2)
        pass

# Data operator
class Operator:
    """Data operator (parent class).

    Attributes:
        id (string): Unique operator ID.
        name (string): Operator name.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    def __init__(self, id=None, name=None, track_prov=False,
                                           propagate_prov=False):
        self.id = _generate_uuid() if id is None else id
        self.name = "Undefined" if name is None else name
        self.track_prov = track_prov
        self.propagate_prov = propagate_prov
        logger.debug("Created {} operator with id {}".format(self.name,
                                                             self.id))

    # NOTE (john): Must be implemented by the subclasses
    def get_next(self):
        logger.error("Method not implemented!")

    # NOTE (john): Must be implemented by the subclasses
    def lineage(self, tuples: List[ATuple]) -> List[List[ATuple]]:
        logger.error("Lineage method not implemented!")

    # NOTE (john): Must be implemented by the subclasses
    def where(self, att_index: int, tuples: List[ATuple]) -> List[List[Tuple]]:
        logger.error("Where-provenance method not implemented!")

# Scan operator
class Scan(Operator):
    """Scan operator.

    Attributes:
        filepath (string): The path to the input file.
        filter (function): An optional user-defined filter.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """

    # Initializes scan operator
    def __init__(self, filepath, filter=None, track_prov=False,
                                              propagate_prov=False):
        super(Scan, self).__init__(name="Scan", track_prov=track_prov,
                                   propagate_prov=propagate_prov)
        # Initialize the fields
        self.filepath = filepath
        self.filter = filter
        """
        local fields of Scan operator:
        end_of_file (bool) : detect whether it reaches the end of the file
        batch_size (int) : number of tuples to ouput in each get_next() call
        curr (int): index of the current file line, starting from line 0.
        read_file: get the input from file
        """
        self.end_of_file = False
        self.curr=0
        self.batch_size=5000
        self.read_file = []

    # retrieve the file
    def __get_file(self):
        # read each line from the file and split the empty space 
        try:
            lst = []
            if not self.filepath:
                raise ValueError("empty filepath")
            with open(self.filepath,"r",newline = '') as f:
                reader = csv.reader(f,delimiter=' ')
                lst = list(reader)
            f.close()
            return lst
        except ValueError as e:
            logger.error(e)


    # Returns next batch of tuples in given file (or None if file exhausted)
    def get_next(self):
        logger.debug("Scan: at get_next()")
        if self.end_of_file: 
            logger.debug("end of file")
            return None
        # initialize a batch
        batch = []
        # process each line from the file to a tuple, and add it to the block
        if self.read_file == []:
            self.read_file = self.__get_file()
        block = self.read_file[self.curr:self.curr+self.batch_size]
        # block size less than batch size means we are at the end of file
        if len(block) < self.batch_size:
            self.end_of_file = True
        for row in block:
            res = []
            for w in row:
                res.append(w)
            t = ATuple(tuple=tuple(res),operator=self)

            if t.tuple is not None and (self.filter is None or self.filter(t.tuple)):
                batch.append(t)
        self.curr += self.batch_size
        return batch
        
            

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

# Equi-join operator
class Join(Operator):
    """Equi-join operator.

    Attributes:
        left_input (Operator): A handle to the left input.
        right_input (Operator): A handle to the left input.
        left_join_attribute (int): The index of the left join attribute.
        right_join_attribute (int): The index of the right join attribute.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    
    
    # Initializes join operator
    def __init__(self, left_input, right_input, left_join_attribute,
                                                right_join_attribute,
                                                track_prov=False,
                                                propagate_prov=False):
        super(Join, self).__init__(name="Join", track_prov=track_prov,
                                   propagate_prov=propagate_prov)
        """properties:
        hashtable: hashtable for storing 
        joinedTuple: joined result 
        hasJoinedTuple: check if joinedTuple is completed
        curr:current position at joinedtuples
        batch_size: size of output from get_next()
        end_of_batch: bool var to tell if we are at the end of batch
        """
        self.left_input = left_input
        self.right_input = right_input
        self.left_join_attribute = left_join_attribute
        self.right_join_attribute = right_join_attribute
        self.hashtable = dict()
        self.hasBuiltTable = False
        self.joinedTuple = []
        self.hasJoinedTuple = False
        self.curr = 0
        self.batch_size = 5000
        self.end_of_batch = False

    # Returns next batch of joined tuples (or None if done)
    def get_next(self):
        # declare a batch
        batch = []
        logger.debug("Join: at get_next()")
        if not self.hasBuiltTable:
            # First, we load up all the tuples from the right input into the hash table
            while True:
                right_upstream = self.right_input.get_next()
                if right_upstream == None:
                    break
                for r in right_upstream:
                    key = r.tuple[self.right_join_attribute]
                    if key in self.hashtable:
                        self.hashtable[r.tuple[self.right_join_attribute]].append(r.tuple)
                    else:
                        self.hashtable[key] = [r.tuple]
            logger.debug("right input done")
        self.hasBuiltTable = True
        # Then for each tuple in the left input, we match and yield the joined output tuple to batch
        left_upstream = self.left_input.get_next()
        if left_upstream == None:
            return None
        for l in left_upstream:
            key = l.tuple[self.left_join_attribute]
            if key in self.hashtable:
                for r in self.hashtable[key]:
                    output = list(l.tuple)
                    output.extend(list(r))
                    logger.debug(str(("in outputlist are: ", output)))
                    batch.append(ATuple(tuple=tuple(output),operator=self))
        logger.debug("left input batch done")

        return batch

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

# Project operator
class Project(Operator):
    """Project operator.

    Attributes:
        input (Operator): A handle to the input.
        fields_to_keep (List(int)): A list of attribute indices to keep.
        If empty, the project operator behaves like an identity map, i.e., it
        produces and output that is identical to its input.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """

    # Initializes project operator
    def __init__(self, input, fields_to_keep=[], track_prov=False,
                                                 propagate_prov=False):
        super(Project, self).__init__(name="Project", track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        # Initialize the fields
        self.input = input
        self.fields_to_keep=fields_to_keep

    # Return next batch of projected tuples (or None if done)
    def get_next(self):
        logger.debug("Project: at get_next()")
        lst = []
        # if upstream pass None, return None
        next_batch = self.input.get_next()
        if next_batch == None: 
            logger.debug("Project done.")
            return None
        # if fields to keep is empty, we assign to batch directly to list
        if self.fields_to_keep == []:
            lst = next_batch
        if lst == []:
            for t in next_batch:
                # convert to lst
                l= list(t.tuple)
                # get a list of fields to remove, and then delete those fields
                fields_to_remove = [i for i in range(len(l)) if i not in self.fields_to_keep]
                for idx in sorted(fields_to_remove, reverse= True):
                    del l[idx]

                # convert back to tuple and yield
                t.tuple = tuple(l)
                t.operator=self
                lst.append(t)
        return lst
        

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

# Group-by operator
class GroupBy(Operator):
    """Group-by operator.

    Attributes:
        input (Operator): A handle to the input
        key (int): The index of the key to group tuples.
        value (int): The index of the attribute we want to aggregate.
        agg_fun (function): The aggregation function (e.g. AVG)
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    

    # Initializes average operator
    def __init__(self, input, key, value, agg_fun, track_prov=False,
                                                   propagate_prov=False):
        super(GroupBy, self).__init__(name="GroupBy", track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        """properties:
        res: total aggregated tuples
        hasAllTuples: if all the tuples are gathered from upstream
        curr:current position at joinedtuples
        batch_size: size of output from get_next()
        end_of_batch: bool var to tell if we are at the end of batch
        """
    
        self.input = input
        self.key = key
        self.value = value
        self.agg_fun = agg_fun
        self.res = []
        self.hasAllTuples = False
        self.curr = 0
        self.batch_size = 5000
        self.end_of_batch = False

    # Returns aggregated value per distinct key in the input (or None if done)
    def get_next(self):
        logger.debug("GroupBy: at get_next()")
        # declare a batch
        batch = []
        if self.end_of_batch: 
            self.res = []

            return None
        if self.key == None:            
            # first we set up an appropriate init value for aggregate
            aggr = []
            if not self.hasAllTuples:
                # pulling from upstream
                while True:
                    upstream = self.input.get_next()
                    if upstream == None: 
                        logger.debug("exit loop")
                        break
                    # then for each tuple, we update the tuple 
                    for atuple in upstream:
                        self.res.append(int(atuple.tuple[self.value]))
            #all tuples upstream are gathered
            self.hasAllTuples = True
            # let aggregate function handle the aggregated data and convert to a tuple
            aggr_res = [str(self.agg_fun(self.res))]
            res = tuple(aggr_res)
            output = ATuple(tuple = res, operator = self)
            self.end_of_batch = True
            return [output]
        else:
            # for each key of the group by attribute, we should return a 2-tuple(key, aggr_val)
            # where aggregate value is the value of the aggregate for the qgourp of tuples corresponding to the key
            # use a dict to keep track of all groups
            aggr = dict()
            if self.end_of_batch == None: 
                logger.debug("Groupby done")
                self.res = []
                return None
            # gather all tuples upstream
            if not self.hasAllTuples:
                while True:
                    upstream = self.input.get_next()
                    #if done with upstream fetching, exit loop
                    if upstream == None: break
                    for t in upstream:
                        g_attr = t.tuple[self.key]
                        # initialize it if not in the dictionary
                        if g_attr not in aggr:
                            aggr[g_attr] = []
                        # for that attribute, update the corresponding aggregate value
                        aggr[g_attr].append(int(t.tuple[self.value]))
                logger.debug(str(("upstreamtuple in groupby: ", aggr)))
                logger.debug("exit pulling")
                # pass it to aggregate function and yield output tuple one by one
                for g_attr in aggr:
                    output = tuple([g_attr, str(self.agg_fun(aggr[g_attr]))])
                    self.res.append(ATuple(tuple=output,operator=self))
                self.hasAllTuples = True
            # we return correct batch size
            if len(self.res) < self.batch_size:
                self.end_of_batch = True
                return self.res
            else:
                batch = self.res[self.curr:self.curr + self.batch_size]
                self.curr += self.batch_size
                self.res = self.res[self.curr:]
                return batch


    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

# Custom histogram operator
class Histogram(Operator):
    """Histogram operator.

    Attributes:
        input (Operator): A handle to the input
        key (int): The index of the key to group tuples. The operator outputs
        the total number of tuples per distinct key.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    

    # Initializes histogram operator
    def __init__(self, input, key=0, track_prov=False, propagate_prov=False):
        super(Histogram, self).__init__(name="Histogram",
                                        track_prov=track_prov,
                                        propagate_prov=propagate_prov)
        # initialize fields
        self.input = input
        self.key = key
        """properties:
        res: total tuples from upstream
        histo: hashmap for 
        hasAllTuples: if all the tuples are gathered from upstream
        curr:current position at joinedtuples
        batch_size: size of output from get_next()
        end_of_batch: bool var to tell if we are at the end of batch
        """
        self.res = []
        self.histo = dict()
        self.hasAllTuples = False
        self.curr = 0
        self.batch_size = 5000
        self.end_of_batch = False

    # Returns histogram (or None if done)
    def get_next(self):

        logger.debug("Histogram at get_next()")
        if self.end_of_batch: return None
        if not self.hasAllTuples:
            while True:
                upstream = self.input.get_next()
                #fetching done? then break out the loop
                if upstream == None: break
                for t in upstream:
                    # key tuple to group by
                    k = t.tuple[self.key]
                    if not k in self.histo:
                        self.histo[k] = [t]
                    else:
                        self.histo[k].append(t)
            for key in self.histo:
                output = tuple([key, len(self.histo[key])])
                self.res.append(ATuple(tuple = output, operator = self))
            # prevent pulling again next time we call get_next()
            self.hasAllTuples=True
        
        # we return correct batch size
        if len(self.res) < self.batch_size:
            self.end_of_batch = True
            return self.res
        else:
            #slice the result to limit to batch
            batch = self.res[self.curr:self.curr + self.batch_size]
            self.curr += self.batch_size
            self.res = self.res[self.curr:]
            return batch


# Order by operator
class OrderBy(Operator):
    """OrderBy operator.

    Attributes:
        input (Operator): A handle to the input
        comparator (function): The user-defined comparator used for sorting the
        input tuples.
        ASC (bool): True if sorting in ascending order, False otherwise.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    
    # Initializes order-by operator
    def __init__(self, input, comparator, ASC=True, track_prov=False,
                                                    propagate_prov=False):
        super(OrderBy, self).__init__(name="OrderBy",
                                      track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        # Initialize the field
        self.input = input
        self.comparator = comparator
        self.ASC = ASC
        """properties:
        res: total sorted tuples
        hasAllTuples: if upstream tuples are gathered
        curr:current position at joinedtuples
        batch_size: size of output from get_next()
        end_of_batch: bool var to tell if we are at the end of batch
        """
        self.res = []
        self.hasAllTuples = False
        self.curr = 0
        self.batch_size = 5000
        self.end_of_batch = False


    # Returns the sorted input (or None if done)
    def get_next(self):
        logger.debug("OrderBy at get_next()")
        # declare a batch
        batch = []
        #if we are done with all input
        if self.end_of_batch: 
            logger.debug("orderby done")
            return None
        # fetch all the batches upstream
        if not self.hasAllTuples:
            while True:
                upstream = self.input.get_next()
                #fetching done? then break out the loop
                if upstream == None: break
                self.res += upstream
        # upstream pulling done
        logger.debug("upstream pulling at Orderby Done")
        self.hasAllTuples=True
        # sort the whole result before we output batch
        self.res.sort(key = self.comparator, reverse = not self.ASC)
        # we return correct batch size
        if len(self.res) < self.batch_size:
            self.end_of_batch = True
            return self.res
        else:
            batch = self.res[self.curr:self.curr + self.batch_size]
            self.curr += self.batch_size
            self.res = self.res[self.curr:]
            return batch



    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

# Top-k operator
class TopK(Operator):
    """TopK operator.

    Attributes:
        input (Operator): A handle to the input.
        k (int): The maximum number of tuples to output.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    
    # Initializes top-k operator
    def __init__(self, input, k=None, track_prov=False, propagate_prov=False):
        super(TopK, self).__init__(name="TopK", track_prov=track_prov,
                                   propagate_prov=propagate_prov)
        # initialize the field
        self.input = input
        self.limit = k
        """properties:
        res: total tuples from upstream
        hasAllTuples: total tuples from upstream
        curr:current position at joinedtuples
        batch_size: size of output from get_next()
        end_of_batch: bool var to tell if we are at the end of batch
        """
        self.res = []
        self.hasAllTuples = False
        self.curr = 0
        self.batch_size = 5000
        self.end_of_batch = False
    # Returns the first k tuples in the input (or None if done)
    def get_next(self):
        logger.debug("TopK at get_next()")
        # declare a batch
        batch = []
        #if we are done with all input
        if self.end_of_batch: return None
        # fetch all the batches upstream
        if not self.hasAllTuples:
            while True:
                upstream = self.input.get_next()
                #fetching done? then break out the loop
                if upstream == None: break
                self.res += upstream
        # prevent pulling again next time we call get_next()
        self.hasAllTuples = True
        self.res = self.res[:self.limit]
        if len(self.res) <= self.batch_size:
            self.end_of_batch = True
            return self.res
        else:
            batch = self.res[self.curr:self.curr + self.batch_size]
            self.curr += self.batch_size
            self.res = self.res[self.curr:]
            return batch


    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

# Filter operator
class Select(Operator):
    """Select operator.

    Attributes:
        input (Operator): A handle to the input.
        predicate (function): The selection predicate.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes select operator
    def __init__(self, input, predicate, track_prov=False,
                                         propagate_prov=False):
        super(Select, self).__init__(name="Select", track_prov=track_prov,
                                     propagate_prov=propagate_prov)
        # Initialize the field
        self.input = input
        self.predicate = predicate

    # Returns next batch of tuples that pass the filter (or None if done)
    def get_next(self):
        try:
            logger.debug("Select: at get_next()")
            # throw exception if we don't have predicate function available
            if not self.predicate:
                raise ValueError("no predicate function")
            # fetch the batch from scanned input, process each tuple to a new batch
            batch = []
            block= self.input.get_next()
            # if nothing from input, return None
            if block == None:
                return None
            # process each block of tuples
            for atuple in block:
                # verify if the tuple satisfies the predicate
                if atuple is not None and self.predicate(atuple.tuple):
                    atuple.operator= self
                    batch.append(atuple)
            return batch
        except ValueError as e:
            logger.error(e)


if __name__ == "__main__":


    logger.info("Assignment #1")
    #build CLI
    parser = argparse.ArgumentParser(description="CS591L1 Assignment #1.\nTask #1: 'likeness' of a movie for user A and Movie M\nTask #2: recommend a movie for user A\nTask #3: explanation query that amounts to histogram of the ratings for movie M as given by user A's friends\n", formatter_class=RawTextHelpFormatter)
    parser.add_argument("-t", "--task", metavar="[task_number]", type=int, required=True, help="Task # to run", dest="task_num")
    parser.add_argument("-f", "--friends", metavar="[path_to_friends_txt]", type=str, required=True, help="Path to friends.txt", dest="friendFile")
    parser.add_argument("-r", "--ratings", metavar="[path_to_ratings_txt]", type=str, required=True, help="Path to movie_ratings.txt", dest="ratingFile")
    parser.add_argument("-u", "--uid", metavar="[user_id]", type=int, required=True, help="User id for task 1, 2 and 3", dest="uid")
    parser.add_argument("-m", "--mid", metavar="[movie_id]", nargs='?',required=False, type=int, help="Movie id for task 1 and 3, movie id is not used in task 2", dest="mid")
    
    args = parser.parse_args()

    logger.info("Starting task #{}.....".format(args.task_num))
    userInfo ="user id={} ,".format(args.uid)
    movieInfo= ("movie id={}, ".format(args.mid), "")[args.task_num == 2]
    filepathInfo = "filepath to friends={}, filepath to ratings={}".format(args.friendFile, args.ratingFile)
    logger.info(userInfo + movieInfo + filepathInfo)
    


    # TASK 1: Implement 'likeness' prediction query for User A and Movie M
    #
    # SELECT AVG(R.Rating)
    # FROM Friends as F, Ratings as R
    # WHERE F.UID2 = R.UID
    #       AND F.UID1 = 'A'
    #       AND R.MID = 'M'

    #user-define predicate function1
    def predicate1(input):
        return input[0]== str(args.uid)
    def predicate2(input):
        return input[1]== str(args.mid)
    def aggrFunction(input):
        return round(sum(input)/len(input),2)

    def task1():
        # push the filter down to leaf
        scanF = Scan(filepath=args.friendFile,filter=predicate1)
        scanR = Scan(filepath=args.ratingFile,filter=predicate2)
        testJoin = Join(left_input=scanF,right_input=scanR,left_join_attribute=1,right_join_attribute=0)
        testGroupby = GroupBy(input=testJoin,key=None,value=4,agg_fun=aggrFunction)
        while True:
            batch = testGroupby.get_next()
            if batch == None: break
            aggr_val = batch[0].tuple[0]
        logger.info("Average rating is "+ aggr_val)



    # TASK 2: Implement recommendation query for User A
    #
    # SELECT R.MID
    # FROM ( SELECT R.MID, AVG(R.Rating) as score
    #        FROM Friends as F, Ratings as R
    #        WHERE F.UID2 = R.UID
    #              AND F.UID1 = 'A'
    #        GROUP BY R.MID
    #        ORDER BY score DESC
    #        LIMIT 1 )

    def task2():
        friends = Scan(filepath=args.friendFile,filter = predicate1)
        ratings = Scan(filepath=args.ratingFile)
        joinTuple = Join(left_input=friends,right_input=ratings,left_join_attribute=1, right_join_attribute=0)
        groupByMid = GroupBy(input=joinTuple,key=3, value= 4,agg_fun=aggrFunction)
        orderByScore = OrderBy(input=groupByMid,comparator = lambda x: x.tuple[1],ASC=False)
        limit = TopK(input=orderByScore,k = 1)
        select = Project(input=limit,fields_to_keep=[0])
        while True:
            batch = select.get_next()
            if batch == None: break
            expected_val = batch[0].tuple[0]
        logger.info("Recommended movie for you is "+ expected_val)


    # TASK 3: Implement explanation query for User A and Movie M
    #
    # SELECT HIST(R.Rating) as explanation
    # FROM Friends as F, Ratings as R
    # WHERE F.UID2 = R.UID
    #       AND F.UID1 = 'A'
    #       AND R.MID = 'M'

    def task3():
        friends = Scan(filepath=args.friendFile,filter = predicate1)
        ratings = Scan(filepath=args.ratingFile,filter = predicate2)
        joinTuple = Join(left_input=friends,right_input=ratings,left_join_attribute=1, right_join_attribute=0)
        histo = Histogram(input=joinTuple,key = 4)
        while True:
            batch = histo.get_next()
            if batch == None: break
            for t in batch:
                logger.info("Movie {} rated {} given by {} friends".format(args.mid,t.tuple[0], t.tuple[1]))
        


    # TASK 4: Turn your data operators into Ray actors
    #
    # NOTE (john): Add your changes for Task 4 to a new git branch 'ray'


    eval("task" + str(args.task_num))()

    logger.info("Assignment #2")

    # TASK 1: Implement lineage query for movie recommendation

    # YOUR CODE HERE


    # TASK 2: Implement where-provenance query for 'likeness' prediction

    # YOUR CODE HERE


    # TASK 3: Implement how-provenance query for movie recommendation

    # YOUR CODE HERE


    # TASK 4: Retrieve most responsible tuples for movie recommendation

    # YOUR CODE HERE
