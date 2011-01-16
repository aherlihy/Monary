import random
import pymongo

c = pymongo.Connection("localhost")
collection = c.monary_test.collection

# 3500 batches * 1000 per batch = 3.5 million records

for i in xrange(3500):
    stuff = [ ]
    for j in xrange(1000):
        record = dict(x1=random.uniform(0, 1),
                      x2=random.uniform(0, 2),
                      x3=random.uniform(0, 3),
                      x4=random.uniform(0, 4),
                      x5=random.uniform(0, 5)
                 )
        stuff.append(record)
    collection.insert(stuff)
