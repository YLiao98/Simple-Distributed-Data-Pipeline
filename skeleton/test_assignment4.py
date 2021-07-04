from assignment_ray import *
import sys
import time
from jaeger_client import Config
from opentracing.ext import tags

ray.init()
def predicate(input):
    return input[0] == "8"
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



friends = Scan.remote(filepath='../data/friends_short.txt',tracing = False,filter = predicate,track_prov = True)
ratings = Scan.remote(filepath='../data/ratings.txt',tracing = False, track_prov = True)
joinTuple = Join.remote(left_input=friends,right_input=ratings,left_join_attribute=1, right_join_attribute=0,tracing = False, track_prov = True)
groupByMid = GroupBy.remote(input=joinTuple,key=3, value= 4,agg_fun=testAgg,tracing = False, track_prov = True)
orderByScore = OrderBy.remote(input=groupByMid,comparator = lambda x: x.tuple[1],ASC=False,tracing = False, track_prov = True)
limit = TopK.remote(input=orderByScore,k = 1,tracing = False,track_prov = True)
select = Project.remote(input=limit,fields_to_keep=[0],tracing = False, track_prov = True)
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