from __future__ import absolute_import
from __future__ import annotations
from __future__ import division
from __future__ import print_function

import csv
import logging
from typing import List, Tuple
import uuid

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
    
    local fields of Scan operator:
        end_of_file (bool) : detect whether it reaches the end of the file
        batch_size (int) : number of tuples to ouput in each get_next() call
        curr (int): index of the current file line, starting from line 0.
        read_file: get the input from file
    """
    end_of_file = False
    curr=0
    batch_size=5000
    read_file = []

    # Initializes scan operator
    def __init__(self, filepath, filter=None, track_prov=False,
                                              propagate_prov=False):
        super(Scan, self).__init__(name="Scan", track_prov=track_prov,
                                   propagate_prov=propagate_prov)
        # Initialize the fields
        self.filepath = filepath
        self.filter = filter
    # retrieve the file
    def get_file(self):
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
            self.read_file = self.get_file()
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
    """class variable:
    hashtable: hashtable for storing 
    joinedTuple: joined result 
    hasJoinedTuple: check if joinedTuple is completed
    curr:current position at joinedtuples
    batch_size: size of output from get_next()
    end_of_batch: bool var to tell if we are at the end of batch
    """
    hashtable = dict()
    joinedTuple = []
    hasJoinedTuple = False
    curr = 0
    batch_size = 5000
    end_of_batch = False
    # Initializes join operator
    def __init__(self, left_input, right_input, left_join_attribute,
                                                right_join_attribute,
                                                track_prov=False,
                                                propagate_prov=False):
        super(Join, self).__init__(name="Join", track_prov=track_prov,
                                   propagate_prov=propagate_prov)
        # Initialize the fields
        self.left_input = left_input
        self.right_input = right_input
        self.left_join_attribute = left_join_attribute
        self.right_join_attribute = right_join_attribute
        

    # Returns next batch of joined tuples (or None if done)
    def get_next(self):
        if self.end_of_batch: return None
        # declare a batch
        batch = []
        logger.debug("Join: at get_next()")
        if not self.hasJoinedTuple:
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
            # Then for each tuple in the left input, we match and yield the joined output tuple to batch
            while True:
                left_upstream = self.left_input.get_next()
                if left_upstream == None:
                    break
                for l in left_upstream:
                    key = l.tuple[self.left_join_attribute]
                    if key in self.hashtable:
                        for r in self.hashtable[key]:
                            output = list(l.tuple)
                            output.extend(list(r))
                            self.joinedTuple.append(ATuple(tuple=tuple(output),operator=self))
            logger.debug("left input done")
            self.hasJoinedTuple = True
        logger.debug("joined tuples should be done.")
        if len(self.joinedTuple) < self.batch_size:
            self.end_of_batch = True
            return self.joinedTuple
        else:
            batch = self.joinedTuple[self.curr:self.curr + self.batch_size]
            self.curr += self.batch_size
            self.joinedTuple = self.joinedTuple[self.curr:]
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
        if next_batch == None: return None
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
    """class variable:
    res: total aggregated tuples
    curr:current position at joinedtuples
    batch_size: size of output from get_next()
    end_of_batch: bool var to tell if we are at the end of batch
    """
    res = []
    curr = 0
    batch_size = 5000
    end_of_batch = False

    # Initializes average operator
    def __init__(self, input, key, value, agg_fun, track_prov=False,
                                                   propagate_prov=False):
        super(GroupBy, self).__init__(name="GroupBy", track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        # Initialize the field
        self.input = input
        self.key = key
        self.value = value
        self.agg_fun = agg_fun

    # Returns aggregated value per distinct key in the input (or None if done)
    def get_next(self):
        logger.debug("GroupBy: at get_next()")
        # declare a batch
        batch = []
        if self.key == None:            
            # first we set up an appropriate init value for aggregate
            aggr = []
            while True:
                upstream = self.input.get_next()
                if upstream == None: 
                    logger.debug("exit loop")
                    break
                # then for each tuple, we update the tuple 
                for atuple in upstream:
                    self.res.append(int(atuple.tuple[self.value]))
            # let aggregate function handle the aggregated data and convert to a tuple
            aggr_res = [self.agg_fun(self.res)]
            res = tuple(aggr_res)
            output = ATuple(tuple = res, operator = self)
            self.end_of_batch = True
            return [output]
        else:
            # for each key of the group by attribute, we should return a 2-tuple(key, aggr_val)
            # where aggregate value is the value of the aggregate for the qgourp of tuples corresponding to the key
            # use a dict to keep track of all groups
            aggr = dict()
            while True:
                upstream = self.input.get_next()
                if upstream == None: break
                for t in upstream:
                    g_attr = t.tuple[self.key]
                    # initialize it if not in the dictionary
                    if g_attr not in aggr:
                        aggr[g_attr] = []
                    # for that attribute, update the corresponding aggregate value
                    aggr[g_attr].append(int(t.tuple[self.value]))
            # pass it to aggregate function and yield output tuple one by one
            for g_attr in aggr:
                output = tuple(g_attr, int(self.agg_fun(aggr[g_attr])))
                self.res.append(ATuple(tuple=output,operator=self))
            
            if len(self.res) < self.batch_size:
                self.end_of_batch = True
                return self.joinedTuple
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
        # YOUR CODE HERE
        pass

    # Returns histogram (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        pass

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
        # YOUR CODE HERE
        pass

    # Returns the sorted input (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        pass

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
        # YOUR CODE HERE
        pass

    # Returns the first k tuples in the input (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        pass

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

    # TASK 1: Implement 'likeness' prediction query for User A and Movie M
    #
    # SELECT AVG(R.Rating)
    # FROM Friends as F, Ratings as R
    # WHERE F.UID2 = R.UID
    #       AND F.UID1 = 'A'
    #       AND R.MID = 'M'

    # YOUR CODE HERE


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

    # YOUR CODE HERE


    # TASK 3: Implement explanation query for User A and Movie M
    #
    # SELECT HIST(R.Rating) as explanation
    # FROM Friends as F, Ratings as R
    # WHERE F.UID2 = R.UID
    #       AND F.UID1 = 'A'
    #       AND R.MID = 'M'

    # YOUR CODE HERE


    # TASK 4: Turn your data operators into Ray actors
    #
    # NOTE (john): Add your changes for Task 4 to a new git branch 'ray'


    logger.info("Assignment #2")

    # TASK 1: Implement lineage query for movie recommendation

    # YOUR CODE HERE


    # TASK 2: Implement where-provenance query for 'likeness' prediction

    # YOUR CODE HERE


    # TASK 3: Implement how-provenance query for movie recommendation

    # YOUR CODE HERE


    # TASK 4: Retrieve most responsible tuples for movie recommendation

    # YOUR CODE HERE
