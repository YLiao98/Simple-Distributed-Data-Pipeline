from assignment_ray import *
import math
import logging
from typing import List, Tuple
import uuid
import argparse
from argparse import RawTextHelpFormatter 
import ray
import sys
import time
from jaeger_client import Config


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.DEBUG)
    ray.init(address='auto')
    #build CLI
    parser = argparse.ArgumentParser(description="CS591L1 Assignment #4\nTask #2: trace recommendation query\nTask #3: trace provenance for recommendation query", formatter_class=RawTextHelpFormatter)
    parser.add_argument("-t", "--task", metavar="[task_number]", type=int, required=True, help="Task # to run", dest="task_num")
    parser.add_argument("-f", "--friends", metavar="[path_to_friends_txt]", type=str, required=True, help="Path to friends.txt", dest="friendFile")
    parser.add_argument("-r", "--ratings", metavar="[path_to_ratings_txt]", type=str, required=True, help="Path to movie_ratings.txt", dest="ratingFile")
    parser.add_argument("-u", "--uid", metavar="[user_id]", type=int, required=True, help="User id for task 1, 2 and 3", dest="uid")
    parser.add_argument("-sc", "--scan", metavar="[scan_bool]", type=int, required=True, help="trace scan operator", dest="scan")
    parser.add_argument("-j", "--join", metavar="[join_bool]", type=int, required=True, help="trace join operator", dest="join")
    parser.add_argument("-gb", "--groupby", metavar="[groupby_bool]", type=int, required=True, help="trace groupby operator", dest="groupby")
    parser.add_argument("-ob", "--orderby", metavar="[orderby_bool]", type=int, required=True, help="trace orderby operator", dest="orderby")
    parser.add_argument("-tk", "--topk", metavar="[topk_bool]", type=int, required=True, help="trace topk operator", dest="topk")
    parser.add_argument("-p", "--project", metavar="[project_bool]", type=int, required=True, help="trace project operator", dest="project")
    parser.add_argument("-tp", "--trackProv", metavar="[trackProv_bool]", type=int, required=True, help="track provenance or not", dest="trackProv")
  
    
    args = parser.parse_args()
    # setup trace boolean variables
    trace_scan = True
    trace_join = True
    trace_groupby = True
    trace_orderby = True
    trace_topk = True
    trace_project = True
    trackProv = True
    if args.scan != 1:
        trace_scan = False
    if args.join != 1:
        trace_join = False
    if args.groupby != 1:
        trace_groupby = False
    if args.orderby != 1:
        trace_orderby = False
    if args.topk != 1:
        trace_topk = False
    if args.project != 1:
        trace_project = False
    if args.trackProv != 1:
        trackProv = False
    logger.info("Starting task #{}.....".format(args.task_num))
    userInfo ="user id={} ,".format(args.uid)
    filepathInfo = "filepath to friends={}, filepath to ratings={}".format(args.friendFile, args.ratingFile)
    
    logger.info(userInfo + filepathInfo)

    def predicate(input):
            return input[0]== str(args.uid)


    def run_assignment4_task2():
        
        def testAgg(input):
            return round(sum(input)/len(input),1)
        def init_tracer(service):
            logging.getLogger('').handlers = []
            logging.basicConfig(format='%(message)s', level=logging.DEBUG)

            config = Config(
                config={
                    'sampler': {
                        'type': 'const',
                        'param': 1,
                    },
                    'logging': True,
                },
                service_name=service,
            )

            # this call also sets opentracing.tracer
            return config.initialize_tracer()
        tracer = init_tracer('queries-test')

        with tracer.start_span('top-level-recomm-query') as query_span:
            friends = Scan.remote(filepath=args.friendFile,tracing = trace_scan,filter = predicate,track_prov = trackProv)
            ratings = Scan.remote(filepath=args.ratingFile,tracing = trace_scan, track_prov = trackProv)
            joinTuple = Join.remote(left_input=friends,right_input=ratings,left_join_attribute=1, right_join_attribute=0,tracing = trace_join, track_prov = trackProv)
            groupByMid = GroupBy.remote(input=joinTuple,key=3, value= 4,agg_fun=testAgg,tracing = trace_groupby, track_prov = trackProv)
            orderByScore = OrderBy.remote(input=groupByMid,comparator = lambda x: x.tuple[1],ASC=False,tracing = trace_orderby, track_prov = trackProv)
            limit = TopK.remote(input=orderByScore,k = 1,tracing = trace_topk,track_prov = trackProv)
            select = Project.remote(input=limit,fields_to_keep=[0],tracing = trace_project, track_prov = trackProv)
            while True:
                batch = ray.get(select.get_next.remote(query_span.context))
                if batch == None: break
                expected_val = batch[0].tuple[0]
            logger.info("Recommended movie for you is "+ expected_val)

        time.sleep(4)

        tracer.close()

    def run_assignment4_task3():
        def testAgg(input):
            return round(sum(input)/len(input),1)
        def init_tracer(service):
            logging.getLogger('').handlers = []
            logging.basicConfig(format='%(message)s', level=logging.DEBUG)

            config = Config(
                config={
                    'sampler': {
                        'type': 'const',
                        'param': 1,
                    },
                    'logging': True,
                },
                service_name=service,
                validate=True,
            )

            # this call also sets opentracing.tracer
            return config.initialize_tracer()
        tracer = init_tracer('queries-test')



        friends = Scan.remote(filepath=args.friendFile,tracing = trace_scan,filter = predicate,track_prov = True)
        ratings = Scan.remote(filepath=args.ratingFile,tracing = trace_scan, track_prov = True)
        joinTuple = Join.remote(left_input=friends,right_input=ratings,left_join_attribute=1, right_join_attribute=0,tracing = trace_join, track_prov = True)
        groupByMid = GroupBy.remote(input=joinTuple,key=3, value= 4,agg_fun=testAgg,tracing = trace_groupby, track_prov = True)
        orderByScore = OrderBy.remote(input=groupByMid,comparator = lambda x: x.tuple[1],ASC=False,tracing = trace_orderby, track_prov = True)
        limit = TopK.remote(input=orderByScore,k = 1,tracing = trace_topk,track_prov = True)
        select = Project.remote(input=limit,fields_to_keep=[0],tracing = trace_project, track_prov = True)
        res = []
        while True:
            batch = ray.get(select.get_next.remote(None))
            if batch == None: break
            res += batch
        logger.debug(res)
        first_tuple = res[0]
        with tracer.start_span('top-level-recomm-query') as query_span:
            query_span.set_tag('parent-span','top-level')
            lst = first_tuple.lineage(query_span.context)
            logger.info("lineage for the recommendation is:")
            logger.info(lst)



        time.sleep(4)
        tracer.close()

    eval("run_assignment4_task" + str(args.task_num))()