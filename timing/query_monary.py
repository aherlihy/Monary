from monary import Monary
import numpy

monary = Monary("127.0.0.1")
arrays = monary.query(
    "monary_test",                  # database name
    "collection",                   # collection name
    {},                             # query spec
    ["x1", "x2", "x3", "x4", "x5"], # field names
    ["float64"] * 5                 # field types
)
monary.close()

for array in arrays:                # prove that we did something...
    print numpy.mean(array)
