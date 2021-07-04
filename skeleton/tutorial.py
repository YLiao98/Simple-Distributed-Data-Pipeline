import logging
import sys
import time
import requests
from jaeger_client import Config

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
tracer = init_tracer('first-service')
with tracer.start_span('first-span') as span:
    span.set_tag('first-tag','100')
    
with tracer.start_span('second-span') as span2:
    span2.set_tag('second-tag','90')
    with tracer.start_span('third-span') as span3:
        span3.set_tag('third-tag','80')
        
with tracer.start_span('fourth-span') as span4:
    span4.set_tag('fourth-tag',60)
    with tracer.start_span('fifth-span',child_of=span4) as span5:
        span5.set_tag('fifth-tag','80')
        


homepages = []
res = requests.get('https://jobs.github.com/positions.json?description=python')

for result in res.json():
    print('Getting website for %s' % result['company'])
    try:
        homepages.append(requests.get(result['company_url']))
    except:
        print('Unable to get site for %s' % result['company'])
        
with tracer.start_span('get-python-jobs') as span:
    homepages = []
    res = requests.get('https://jobs.github.com/positions.json?description=python')
    span.set_tag('jobs-count',len(res.json()))
    for result in res.json():
        with tracer.start_span(result['company'],child_of=span) as site_span:
            print('Getting website for %s' % result['company'])
            try:
                homepages.append(requests.get(result['company_url']))
            except:
                print('Unable to get site for %s' % result['company'])
                
with tracer.start_span('get-python-jobs') as span:
    homepages = []
    res = requests.get('https://jobs.github.com/positions.json?description=python')
    span.set_tag('jobs-count',len(res.json()))
    for result in res.json():
        with tracer.start_span(result['company'],child_of=span) as site_span:
            print('Getting website for %s' % result['company'])
            try:
                homepages.append(requests.get(result['company_url']))
                site_span.set_tag('request-type','Success')
            except:
                print('Unable to get site for %s' % result['company'])
                site_span.set_tag('request-type','Failure')
                
                
time.sleep(2)
tracer.close()