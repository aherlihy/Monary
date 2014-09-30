# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import numpy.random as nprand
import pymongo

from profile import profile

try:
    xrange
except NameError:
    xrange = range

NUM_BATCHES = 4500
BATCH_SIZE = 1000
# 4500 batches * 1000 per batch = 4.5 million records


def do_insert():
    c = pymongo.MongoClient("localhost")
    collection = c.monary_test.collection

    num_docs = NUM_BATCHES * BATCH_SIZE
    arrays = [nprand.uniform(0, i + 1, num_docs) for i in xrange(5)]
    with profile("pymongo insert"):
        for i in xrange(NUM_BATCHES):
            stuff = []
            for j in xrange(BATCH_SIZE):
                idx = i * BATCH_SIZE + j
                record = {"x1": arrays[0][idx],
                          "x2": arrays[1][idx],
                          "x3": arrays[2][idx],
                          "x4": arrays[3][idx],
                          "x5": arrays[4][idx]}
                stuff.append(record)
            collection.insert(stuff)

if __name__ == "__main__":
    do_insert()
    print("Inserted %d records." % (NUM_BATCHES * BATCH_SIZE))
