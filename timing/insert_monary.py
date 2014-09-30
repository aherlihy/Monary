# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import numpy as np
import numpy.ma as ma
import numpy.random as nprand

from monary import Monary, MonaryParam, WriteConcern, MONARY_W_DEFAULT
from profile import profile

NUM_BATCHES = 4500
BATCH_SIZE = 1000
# 4500 batches * 1000 per batch = 4.5 million records


def do_insert():
    m = Monary()
    num_docs = NUM_BATCHES * BATCH_SIZE
    params = [MonaryParam(
        ma.masked_array(nprand.uniform(0, i + 1, num_docs),
                        np.zeros(num_docs)), "x%d" % i) for i in range(5)]
    wc = WriteConcern(w=MONARY_W_DEFAULT)
    with profile("monary insert"):
        m.insert("monary_test", "collection", params, write_concern=wc)

if __name__ == "__main__":
    do_insert()
    print("Inserted %d records." % (NUM_BATCHES * BATCH_SIZE))
