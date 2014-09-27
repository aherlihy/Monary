# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import numpy

import pymongo

from profile import profile

def do_pymongo_query():

    c = pymongo.MongoClient()
    collection = c.monary_test.collection

    with profile("pymongo query"):
        num = collection.count()
        arrays = [ numpy.zeros(num) for i in range(5) ]
        fields = [ "x1", "x2", "x3", "x4", "x5" ]
        arrays_fields = list(zip(arrays, fields))

        for i, record in enumerate(collection.find()):
            for array, field in arrays_fields:
                array[i] = record[field]

    # prove that we did something...
    print(numpy.mean(arrays, axis=-1))

if __name__ == '__main__':
    do_pymongo_query()
