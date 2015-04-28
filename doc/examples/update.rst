Update Example
==============

This example shows you how to use Monary's ``update`` command to modify
documents already in MongoDB.

Any values that can be used to form a "selectors" for remove can also form be
used to form selectors for update. Similarly any values used to form documents
for insert can be used to from documents for update.

Similarly to :doc:`remove </examples/remove>` a single Monary update command
runs a MongoDB update commands for each element in the data arrays.

.. seealso::

    :doc:`Monary's Insert Example </examples/insert>`,
    :doc:`Monary's Remove Example </examples/remove>`, and
    `the MongoDB update reference
    <http://docs.mongodb.org/manual/reference/method/db.collection.update/>`_

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


Using Monary Update
-------------------
Suppose later we want to normalize all of these values to a standard normal. We
first query to retrieve our data::

    >>> ids, scores = client.query("scores", "data", {},
    ...                            ["_id", "score"], ["id", "float64"])

Now we normalize our scores::

    >>> std, mean = scores.std(), scores.mean()
    >>> scores = (scores - mean) / std

Next we must create the ``MonaryParam``\ s before we perform the update::

    >>> ids = MonaryParam(ids, "_id", "id")
    >>> scores = MonaryParam(scores, "$set.score")

Finally, we can use Monary to update all of our old scores to be the normalized
data instead::

    >>> num_updated = client.update("scores", "data", [ids], [scores])
    >>> num_updated
    10000

These values have all been updated in our database, and we can see that ten
thousand documents were modified.
