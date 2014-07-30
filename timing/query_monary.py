# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import numpy

from monary import Monary
from profile import profile

def do_monary_query():
    with Monary("127.0.0.1") as m:
        with profile("monary query"):
            arrays = m.query(
                "monary_test",                  # database name
                "collection",                   # collection name
                {},                             # query spec
                ["x1", "x2", "x3", "x4", "x5"], # field names
                ["float64"] * 5                 # field types
            )

    # prove that we did something...
    print(numpy.mean(arrays, axis=-1))

if __name__ == '__main__':
    do_monary_query()
