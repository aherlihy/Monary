import numpy
import pymongo

from profile import profile

def do_pymongo_query():

    c = pymongo.Connection("localhost")
    collection = c.monary_test.collection

    with profile("pymongo query"):
        num = collection.count()
        arrays = [ numpy.zeros(num) for i in range(5) ]
        fields = [ "x1", "x2", "x3", "x4", "x5" ]
        arrays_fields = zip(arrays, fields)

        for i, record in enumerate(collection.find()):
            for array, field in arrays_fields:
                array[i] = record[field]

    for array in arrays: # prove that we did something...
        print numpy.mean(array) 

if __name__ == '__main__':
    do_pymongo_query()
