Introduction
============

MongoDB is a document-oriented database, organized for quick access to records
(or rows) of data.  When doing analytics on a large data set, it is often
desirable to have it in a column-oriented format.  Columns of data may be
thought of as mathematical vectors, and a wealth of techniques exist for
gathering statistics about data that is stored in vector form.

For *small to medium* sized collections, it is possible to materialize several
columns of data in the memory of a modern PC.  For example, an array of 100
million double-precision numbers consumes 800 million bytes, or about 0.75 GB.
For larger problems, it's still possible to materialize a substantial portion
of the data, or to work with data in multiple segments.  (Very large problems
require more powerful weapons, such as map/reduce.)

Extracting column data from MongoDB using Python is fairly straightforward.  In
PyMongo, ``collection.find()`` generates a sequence of dictionary objects.
When dealing with millions of records, the trick is not to keep these
dictionaries in memory, as they tend to be large.  Fortunately, it's easy to
move the data in to arrays as it is loaded.

First, let's create 3.5 million rows of test data:

.. code-block:: python
    #!/usr/bin/env python
    import random
    import pymongo

    NUM_BATCHES = 3500
    BATCH_SIZE = 1000
    # 3500 batches * 1000 per batch = 3.5 million records

    c = pymongo.MongoClient()
    collection = c.mydb.collection

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

Here's an example that uses numpy arrays:

.. code-block:: python
    #!/usr/bin/env python
    import numpy
    import pymongo

    c = pymongo.MongoClient()
    collection = c.mydb.collection
    num = collection.count()
    arrays = [ numpy.zeros(num) for i in range(5) ]

    for i, record in enumerate(collection.find()):
        for x in range(5):
            arrays[x][i] = record["x%i" % x+1]

    for array in arrays: # prove that we did something...
        print numpy.mean(array)

With 3.5 million records, this query takes 85 seconds on an EC2 Large instance
running Ubuntu 10.10 64-bit, and takes 88 seconds on my MacBook Pro (2.66 GHz
Intel Core 2 Duo with 8 GB RAM).

These timings might seem impressive, given that they're loading 200,000+ values
per second.  However, closer examination reveals that much of that time is
spent by pymongo as it reads each query result and transforms the BSON result
to a Python dictionary.  (If you watch the CPU usage, you'll see Python is
using 90% or more of the CPU.)

Monary
======

It is possible to get (much) more speed from the query if we bypass the PyMongo
driver.  To demonstrate this, I've developed *monary*, a simple C library and
accompanying Python wrapper which make use of MongoDB C driver.  The code is
designed to accept a list of desired fields, and to load exactly those fields
from the BSON results into some provided array storage.

Here's an example of the same query using monary:

.. code-block:: python
    #!/usr/bin/env python

    from monary import Monary
    import numpy

    with Monary("127.0.0.1") as monary:
        arrays = monary.query(
            "mydb",                         # database name
            "collection",                   # collection name
            {},                             # query spec
            ["x1", "x2", "x3", "x4", "x5"], # field names (in Mongo record)
            ["float64"] * 5                 # Monary field types (see below)
        )

    for array in arrays:                    # prove that we did something...
        print numpy.mean(array)

Monary is able to perform the same query in 4 seconds flat, for a rate of about
4 million values per second (20 times faster!)  Here's a quick summary of how
this Monary query stacks up against PyMongo:

* PyMongo Insert -- EC2: 102 s -- Mac: 76 s
* PyMongo Query -- EC2: 85 s -- Mac: 88 s
* Monary Query -- EC2: 5.4 s -- Mac: 3.8 s

Of course, this test has created some fairly ideal circumstances:  It's
querying for every record in the collection, the records contain only the
queried data (plus ObjectIDs), and the database is running locally.  The
performance may degrade if we used a remote server, if the records were larger,
or if queried for a only subset of the records (requiring either that more
records be scanned, or that an index be used).

Monary now knows about the following types:

* id (Mongo's 12-byte ObjectId)
* int8
* int16
* int32
* int64
* float32
* float64
* bool
* date (stored as int64, milliseconds since epoch)

Monary's source code is available on bitbucket.  It includes a copy of the
Mongo C driver, and requires compilation and installation, which can be done
via the included "setup.py" file.  (The installation script works, but is in a
somewhat rough state.  Any help from a distutils guru would be greatly
appreciated!)  To run Monary from Python, you will need to have the pymongo and
numpy packages installed.

Monary has been slowly gaining functionality (including the recent additions of
more numeric types and the date type). Here are some planned future
improvements:

* Support for string / binary types
 
  (I hope to develop Monary to support some reasonable mapping of most BSON
  types onto array storage.)

* Support for fetching nested fields (e.g. "x.y")

* Remove dependencies on PyMongo and NumPy (possibly)

  (Currently these must be installed in order to use Monary.)
