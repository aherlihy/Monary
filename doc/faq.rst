Frequently Asked Questions
==========================

.. contents::

.. _monary-crud:

Can Monary do Removes, Updates, and/or Upserts?
-----------------------------------------------
Though there will soon be support for bulk removes, updates, and upserts from
arrays into MongoDB, for now Monary can only retrieve and store data. It cannot
perform any updates or removals. In the meantime, you can use
`PyMongo <http://api.mongodb.org/python/current/>`_.

.. _masked-values:

Why does my array contain masked values?
----------------------------------------
Typically, a value is masked if the data type you specify for a field is
incompatible with the actual type retrieved from the document in MongoDB, or
if the specified field is absent in some of your documents.

Alternatively, it could be that the documents do not contain the field that
you requested.

If the entire array is masked, there are no documents in the collection that
contain that field, or all of the matching fields in the database have an
incompatible type.

If there are only some masked values in the result array, then some of the
documents have fields with the specified name but not of the specified type.

Consider, for example, inserting the following two documents at the mongo
shell:

.. code-block:: javascript

    > db.foo.insert({ a : NumberInt(1), sequence : 1 });
    WriteResult({ "nInserted" : 1 })

    > db.foo.insert({ a : "hello", sequence : 2 })
    WriteResult({ "nInserted" : 1 })

Because there is a type mismatch for the field "a", some values will be masked
depending on what type the query asks for::

    >>> from monary import Monary()
    >>> m = Monary()
    >>> m.query("test", "foo", {}, ["a"], ["int32"], sort="sequence")
    [masked_array(data = [1 --],
                 mask = [False  True],
           fill_value = 99999)
    ]
    >>> m.query("test", "foo", {}, ["a"], ["string:5"], sort="sequence")
    [masked_array(data = [-- 'hello'],
                 mask = [ True False],
           fill_value = N/A)
    ]

.. _mvoid-array:

How does monary deal with ObjectIds?
------------------------------------
When querying for ``_id``'s or when inserting documents, you might end up with
ObejectIds stored in a NumPy array. The data type of the array is ``"<V12"``,
and each individual element is of type ``numpy.ma.core.mvoid``. However it may
end up looking like this::

    masked_array(data = [ array([ 83, -18,  62, -28,  97,  21,  83, -51, -21, 106, -54,  11], dtype=int8)
     array([ 83, -18,  62, -28,  97,  21,  83, -51, -21, 106, -54,  12], dtype=int8)
     array([ 83, -18,  62, -28,  97,  21,  83, -51, -21, 106, -54,  13], dtype=int8)
     ...,
     array([  83,  -18,   63,  -63,   97,   21,   83,  -51,  -21, -104, -112,
            -56], dtype=int8)
     array([  83,  -18,   63,  -63,   97,   21,   83,  -51,  -21, -104, -112,
            -55], dtype=int8)
     array([  83,  -18,   63,  -63,   97,   21,   83,  -51,  -21, -104, -112,
            -54], dtype=int8)],
                 mask = [False False False ..., False False False],
           fill_value = ???)

Or it may look like this::

    [masked_array(data = [<read-write buffer ptr 0x7f93718a3600, size 12 at 0x1094e1df0>
    <read-write buffer ptr 0x7f93718a360c, size 12 at 0x1094e1d70>
    <read-write buffer ptr 0x7f93718a3618, size 12 at 0x1094e1f70> ...,
    <read-write buffer ptr 0x7f93718b4f1c, size 12 at 0x1097b92b0>
    <read-write buffer ptr 0x7f93718b4f28, size 12 at 0x1097b92f0>
    <read-write buffer ptr 0x7f93718b4f34, size 12 at 0x1097b9330>],
             mask = [False False False ..., False False False],
       fill_value = ???)
    ]

If you would like this as a ``bson.objectid.ObjectId``, it can be done like
this::

    >>> from monary import Monary
    >>> with Monary() as m:
    ...     ids = m.query("db", "col", {}, ["_id"], ["id"])

    >>> from monary.monary import mvoid_to_bson_id
    >>> id_vals = ids[0] # Depends on the type of query.
    >>> oids = list(map(mvoid_to_bson_id, id_vals))
    >>> oids[0]
    ObjectId('53dba51e61155374af671dc1')

.. _data-types:

What if I don't know what type of data I want from MongoDB?
-----------------------------------------------------------
MongoDB has very flexible schemas; a consequence of this is that documents in
the same collection can have fields of different types. To determine the type
of data for a certain field name, specify the type "type"::

    >>> from monary import Monary
    >>> m = Monary()
    >>> m.query("test", "foo", {}, ["a"], ["type"])
    [masked_array(data = [16 2]
                 mask = [False False],
           fill_value = 999999)
    ]

This returns an 8-bit integer containing the BSON type code for the object.

.. seealso::

    The `BSON specification <http://bsonspec.org/spec.html>`_ for the
    BSON type codes.

.. _using-strings:

How do I retrieve string data using Monary?
-------------------------------------------
Internally, all strings are `C strings
<http://en.wikipedia.org/wiki/C_string#Definitions>`_.  To specify a string
type, you must also indicate the size of the string (not including the
terminating ``NUL`` character)::

    >>> m.query("test", "foo", {}, ["mystr"], ["string:3"])
    [masked_array(data = ['foo' 'bar' 'baz'],
                 mask = [False False False],
           fill_value = N/A)
    ]

Ideally, the size specified should be the least upper bound
of the sizes of strings you are expecting to receive.

.. seealso::

    :doc:`examples/string`

.. _using-block-queries:

When should I use a block query?
--------------------------------
Block query can be used to read through many documents while only storing a
specified amount of documents in memory at a time. This can save memory and
decrease initial latency by processing documents in batches. This can also be
used in combination with insert to perform operations on all of your data and
store the processed results in a new collection.

.. seealso::

    :doc:`examples/block-query` and :doc:`examples/insert`

.. _integer-double-type-code:

Why do my integers have a "double" type code?
---------------------------------------------
Though the numbers look like integers, they are being stored internally as
doubles. This most commonly happens at the mongo shell:

.. code-block:: javascript

    > use test
    > db.foo.insert({ a : 22 })
    WriteResult({ "nInserted" : 1 })

The BSON type code for double is 1, so this results in::

    >>> m.query("test", "foo", {}, ["a"], ["type"])
    [masked_array(data = [1],
                 mask = [False],
           fill_value = N/A)
    ]

Because the mongo shell is a JavaScript interpreter, it follows the rules of
JavaScript: all numbers are floating-point. If you'd like to insert strictly
integers into MongoDB, use ``NumberInt``:

.. code-block:: javascript

    > use test
    > db.foo.insert({ b : NumberInt(1) })
    WriteResult({ "nInserted" : 1 })

This yields the expected type code::

    >>> m.query("test", "foo", {}, ["b"], ["type"])
    [masked_array(data = [16],
                 mask = [False],
           fill_value = N/A)
    ]

.. seealso::

    `ECMAScript Number Type <http://bclary.com/2004/11/07/#a-4.3.20>`_
