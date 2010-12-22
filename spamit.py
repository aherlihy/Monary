import pymongo
import random

c = pymongo.Connection("localhost")

coll = c.test.spam

def make_record():
    record = {
      "name": "foo",
      "intval": random.randint(-20, 20),
      "float1": random.uniform(0.0, 2.0),
      "float2": random.uniform(0.0, 4.0),
      "float3": random.uniform(0.0, 6.0),
      "float4": random.uniform(0.0, 8.0),
      "float5": random.uniform(0.0, 10.0),
      "float6": random.uniform(0.0, 12.0),
      "float7": random.uniform(0.0, 14.0),
      "float8": random.uniform(0.0, 16.0),
      "float9": random.uniform(0.0, 18.0),
      "float10": random.uniform(0.0, 20.0),
    }
    return record

NUM_BATCHES = 10000

for i in xrange(NUM_BATCHES):
    stuff = [ make_record() for j in xrange(1000) ]
    print "inserting batch %i / %i" % (i+1, NUM_BATCHES)
    coll.insert(stuff)
