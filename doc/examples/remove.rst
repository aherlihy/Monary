Remove Example
==============

This example shows you how to use Monary's ``remove`` command to remove
documents from MongoDB.

Any value that can be inserted can also be used to form a "selector" for
remove. Selectors are simply BSON documents. Depending on the value of remove's
``just_one`` command, a single matching document or all matching documents will
be removed for each selector created during monary's ``remove``.

A single Monary remove command runs a MongoDB remove commands for each element
in the data arrays. For example, the monary command::

    values = numpy.ma.masked_array([1, 2, 3], [False] * 3)
    client.remove('db', 'collection', [MonaryParam(values, "fieldname")])

\...is equivalent to these PyMongo operations::

    collection = MongoClient().db.collection
    collection.remove({'fieldname': 1})
    collection.remove({'fieldname': 2})
    collection.remove({'fieldname': 3})

.. seealso::

    :doc:`Monary's Insert Example </examples/insert>` and
    `the MongoDB remove reference
    <http://docs.mongodb.org/manual/reference/method/db.collection.remove/>`_

Purpose of Remove
-----------------
Monary's "bulk" removes are different than regular MongoDB removes in that they
perform many MongoDB removes at once. This gives you a fast way to remove large
amounts of data with one command, while still having fine grained control over
what is removed.

Setup
-----
For this example, we will use Monary's insert to put ten thousand random values
representing scores out of 100 into our database. First, we set up a Monary
connection to the local MongoDB database::

    >>> from monary import Monary, MonaryParam
    >>> client = Monary()

Next, we generate and insert the documents::

    >>> import numpy as np
    >>> # 10,000 numbers from 0 to 100
    >>> scores = np.random.uniform(0, 100, 10000)

    >>> mask = np.zeros(len(scores), dtype="bool")
    >>> scores = np.ma.masked_array(scores, mask)
    >>> ids = client.insert("scores", "data", [MonaryParam(scores, "score")])


Using Monary Remove
-------------------
Suppose we have done our processing and now want to remove the data we inserted
above from the database. We must first make a MonaryParam from the data::

    >>> ids = MonaryParam(ids, "_id", "id")

Now we can perform the removal::

    >>> num_removed = client.remove("scores", "data", [ids])
    >>> num_removed
    10000

Suppose instead of the last two commands above, we had wanted to remove all of
the data we just inserted where the score was less than 80. Suppose also that
we don't want to accidentally remove data that had been in our database before
we started with this example. Then, we can do this with Monary like so::

    >>> arr = np.zeros(len(ids), dtype="float64")
    >>> arr += 80.0
    >>> mask = np.zeros(len(arr), dtype="bool")
    >>> arr = np.ma.masked_array(arr, mask)
    >>> mp = MonarParam(arr, "score.$lt")
    >>> num_removed = client.remove("scores", "data", [ids, mp])
    >>> num_removed
    7974
