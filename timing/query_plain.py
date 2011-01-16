import numpy
import pymongo

c = pymongo.Connection("localhost")
collection = c.monary_test.collection
num = collection.count()

arrays = [ numpy.zeros(num) for i in range(5) ]
fields = [ "x1", "x2", "x3", "x4", "x5" ]
arrays_fields = zip(arrays, fields)

for i, record in enumerate(collection.find()):
    for array, field in arrays_fields:
        array[i] = record[field]

for array in arrays: # prove that we did something...
    print numpy.mean(array) 
