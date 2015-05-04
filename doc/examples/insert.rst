Insert Example
==============

This example shows you how to use Monary's ``insert`` method to send documents
to MongoDB.

Any value that can be queried can also be inserted. Both nested field insertion
(via fields containing ".") and BSON value insertion are supported as well.

Purpose of Insert
-----------------
Inserts allow you to use Monary to convert data from NumPy masked arrays into
documents stored in MongoDB. Monary's insert takes in a list of
``MonaryParams``.

Monary inserts can also be used to store intermediate data. This can be
useful when doing operations on blocks of data with :doc:`block query </examples/block-query>`.

Setup
-----
For this example, let's insert some unprocessed documents representing students'
test scores into MongoDB. Please see the
:doc:`MonaryParam</examples/monary-param>` example to understand how to
create a ``MonaryParam``.

First we need to connect to our local DB::

    >>> import monary
    >>> client = monary.Monary()

Next, we generate the documents. Note that we are using ``bson.encode`` to
store our subdocument::

    >>> import bson
    >>> import random
    >>> al_num = '0123456789abcdefghijklmnopqrstuvwxyz'
    >>> scores = []
    >>> ids = []
    >>> names = []
    >>> for _ in range(1000):
    ...     ids.append("".join(al_num[random.randint(0, len(al_num)-1)]
    ...                          for _ in range(14)))
    ...     score = {"midterm": random.randint(0, 1000) / 10,
    ...              "final": random.randint(0, 1000) / 10}
    ...     scores.append(bson.BSON.encode(score))
    ...     names.append("...")

Now that we have generated documents, we need to construct a ``MonaryParam``.
``MonaryParams`` represent one column, i.e. one field, for a set of BSON documents.
We need the data itself to be in numpy's masked_array type::

    >>> import numpy as np
    >>> max_length = max(map(len, scores))
    >>> scores_ma = np.ma.masked_array(scores, np.zeros(1000), "<V%d"%max_length)
    >>> ids_ma = np.ma.masked_array(ids, np.zeros(1000), "S14")
    >>> names_ma = np.ma.masked_array(names, np.zeros(1000), "S3")

Now we can create a ``MonaryParam``::

    >>> types = ["bson:%d"%max, "string:14", "string:3"]
    >>> fields = ["scores", "student_id", "student_name"]
    >>> values = [scores_ma, ids_ma, names_ma]
    >>> params = monary.MonaryParam.from_lists(values, fields, types)

And we can insert it into the database "monary_students", and the collection "raw"::

    >>> client.insert("monary_students", "raw", params)

Using Monary Insert
-------------------
The semester has ended, and it's time to assign grades to each student.
Let's first get all the raw test data back into NumPy arrays with Monary::

    >>> import numpy as np
    >>> from monary import Monary
    >>> m = Monary()
    >>> ids, midterm, final = \
    ...     m.query("monary_students", "raw", {},
    ...             ["student_id",
    ...              "test_scores.midterm",
    ...              "test_scores.midterm"],
    ...             ["string:14", "float64",
    ...              "float64"])

Now we process the scores and assign grades to each student::

    >>> grades = [None, None]
    >>> for i, arr in enumerate([midterm, final]):
    ...     # curve to average of 2.3333
    ...     mean, stdev = arr.mean(), arr.std()
    ...     grades[i] = (arr - mean) / stdev
    ...     grades[i] += 2.3333
    ...     # bound grades within [0.0, 4.0]
    ...     fours = np.argwhere(grades[i] > 4.0)
    ...     zeros = np.argwhere(grades[i] < 0.0)
    ...     grades[i][fours] = 4.0
    ...     grades[i][zeros] = 0.0

Now weight both tests and assign overall grades::

    >>> overall_grades = (grades[0] * 0.4 + grades[1] * 0.6).round(3)

Then we need to create ``MonaryParams``::

    >>> from monary import MonaryParam
    >>> id_mp = MonaryParam(ids, "student_id", "string:14")
    >>> overall_mp = MonaryParam(overall_grades, "grades.overall")
    >>> midterm_mp = MonaryParam(grades[0], "grades.midterm")
    >>> final_mp = MonaryParam(grades[1], "grades.final_exam")

Finally, we insert the results to the database::

    >>> ids = m.insert("monary_students", "graded",
    ...                [id_mp, overall_mp, midterm_mp, final_mp])
    >>> from monary import mvoid_to_bson_id
    >>> oids = list(map(mvoid_to_bson_id, ids))
    >>> oids[0]
    ObjectId('53dba51e61155374af671dc1')

We can see that insert returns a Numpy array containing the ObjectId of the
inserted documents.
