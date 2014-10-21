# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import random
import pymongo

from profile import profile

try:
    xrange
except NameError:
    xrange = range


def do_insert():

    NUM_BATCHES = 4500
    BATCH_SIZE = 1000
    # 4500 batches * 1000 per batch = 4.5 million records

    c = pymongo.MongoClient()
    collection = c.monary_test.collection

    with profile("insert"):
        for i in xrange(NUM_BATCHES):
            stuff = [ ]
            for j in xrange(BATCH_SIZE):
                record = dict(x1=random.uniform(0, 1),
                              x2=random.uniform(0, 2),
                              x3=random.uniform(0, 3),
                              x4=random.uniform(0, 4),
                              x5=random.uniform(0, 5)
                         )
                stuff.append(record)
            collection.insert(stuff)
    print "Inserted %d records." % (NUM_BATCHES * BATCH_SIZE)

if __name__ == '__main__':
    do_insert()
