Aggregation Pipeline
====================

This example demonstrates how to use MongoDB's `aggregation pipeline
<http://docs.mongodb.org/manual/core/aggregation-introduction/>`_ with
Monary. It assumes that you know the basics of aggregation pipelines. For a
complete list of the possible pipeline operators, refer to the `aggregation
framework operators
<http://docs.mongodb.org/manual/reference/operator/aggregation/>`_.

Note that you must have MongoDB 2.2 or later to use the aggregation pipeline.

Setup
-----
This example assumes that **mongod** is running on the default host and port.
You can use any test data you want to aggregate on; this example will use the
MongoDB `zipcode data set <http://media.mongodb.org/zips.json>`_. Simply use
**mongoimport** to import the collection into MongoDB.

.. code-block:: bash

    $ wget http://media.mongodb.org/zips.json
    $ mongoimport --db zips --collection data zips.json

The data set will be loaded to the database ``zips`` under the collection
``data``:

.. code-block:: javascript

    > use zips
    switched to db zips
    > db.data.find().limit(1).pretty()
    {
        "_id" : "01001",
        "city" : "AGAWAM",
        "loc" : [
            -72.622739,
            42.070206
        ],
        "pop" : 15338,
        "state" : "MA"
    }


Performing Aggregations
-----------------------
In Monary, a pipeline must be a list of Python dicts. Each dict must contain
exactly one aggregation pipeline operator, each representing one stage of the
pipeline.

For convenience, you may also pass a dict containing a single aggregation
operation if your pipeline contains only one stage.

This example will show all of the states in the dataset and their populations::

    >>> from monary import Monary
    >>> m = Monary()
    >>> pipeline = [{"$group" : {"_id" : "$state",
    ...                          "totPop" : {"$sum" : "$pop"}}}]
    >>> states, population = m.aggregate("zips", "data",
    ...                                  pipeline,
    ...                                  ["_id", "totPop"],
    ...                                  ["string:2", "int64"])
    >>> strs = list(map(lambda x: x.decode("utf-8"), states))
    >>> list("%s: %d" % (state, pop)
             for (state, pop) in zip(strs, population))
    ['WA: 4866692',
     'HI: 1108229',
     'CA: 29754890',
     'OR: 2842321',
     'NM: 1515069',
     'UT: 1722850',
     'OK: 3145585',
     'LA: 4217595',
     'NE: 1578139',
     'TX: 16984601',
     'MO: 5110648',
     'MT: 798948',
     'ND: 638272',
     'AK: 544698',
     'SD: 695397',
     'DC: 606900',
     'MN: 4372982',
     'ID: 1006749',
     'KY: 3675484',
     'WI: 4891769',
     ...]
