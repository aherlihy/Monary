# Monary - Copyright 2011-2013 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

from monary import Monary
import numpy

from profile import profile

def do_monary_block_query():
    count = 0
    sums = numpy.zeros((5,))
    with Monary("127.0.0.1") as m:
        with profile("monary block query"):
            for arrays in m.block_query(
                "monary_test",                  # database name
                "collection",                   # collection name
                {},                             # query spec
                ["x1", "x2", "x3", "x4", "x5"], # field names
                ["float64"] * 5,                # field types
                block_size=8192,
                ):
                count += len(arrays[0])
                sums += [ numpy.sum(arr) for arr in arrays ]

    print "visited %i items" % count
    print sums / count                          # prove that we did something...

if __name__ == '__main__':
    do_monary_block_query()
