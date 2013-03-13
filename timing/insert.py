# Monary - Copyright 2011-2013 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import random
import pymongo

from profile import profile

def do_insert():

    NUM_BATCHES = 3500
    BATCH_SIZE = 1000
    # 3500 batches * 1000 per batch = 3.5 million records

    c = pymongo.Connection("localhost")
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

if __name__ == '__main__':
    do_insert()
