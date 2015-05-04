Tutorial
========

Prerequisites
-------------
To start, you need to have **Monary** :doc:`installed <installation>`. In a
Python shell, the following should run without raising an exception::

    >>> import monary

You'll also need a MongoDB instance running on the default host and port
(``localhost:27017``). If you have `downloaded and installed
<http://www.mongodb/org/display/DOCS/Getting+Started>`_ MongoDB, you can start
the mongo daemon in your system shell:

.. code-block:: bash

    $ mongod

Making a Connection with Monary
-------------------------------
To use **Monary**, we need to create a connection to a running **mongod**
instance. We can make a new Monary object::

    >>> from monary import Monary
    >>> client = Monary()

This connects to the default host and port; it can also be specified
explicitly::

    >>> client = Monary("localhost", 27017)

Monary can also accept MongoDB URI strings::

    >>> client = Monary("mongodb://me@password:test.example.net:2500/database?replicaSet=test&connectTimeoutMS=300000")

.. seealso::

    The `MongoDB connection string format
    <http://docs.mongodb.org/manual/reference/connection-string/>`_ for more
    information about how these URI's are formatted.

Performing Finds
----------------
To perform a "find" query, we first need a data set. We can insert some sample data using
the mongo shell:

.. code-block:: bash

    $ mongo

Then, at the shell prompt, insert some sample documents into the collection named "coll":

.. code-block:: javascript

    > use test
    switched to db test
    > for (var i = 0; i < 5000; i++) {
    ... db.coll.insert({ a : Math.random(), b : NumberInt(i) })
    ... }

To check that you've successfully inserted documents, you can run:

.. code-block:: javascript

   > db.coll.find()

Which will print out the first batch of documents. Each document looks something like this:

.. code-block:: javascript

    {
        a : 0.34534613435643535,
        b : 1
    }


To query the database using Monary, you need to specify the name and type of a MongoDB document field.

For example, to retrieve all of the ``b`` values with Monary, we use the ``query()`` function
and specify the field name we want and its type::

    >>> with Monary() as m:
    ...     arrays = m.query("test", "coll", {}, ["b"], ["int32"])

``arrays`` is now a list containing a NumPy `masked array
<http://docs.scipy.org/doc/numpy/reference/maskedarray.generic.html>`_ with 5000
values::

    >>> arrays
    [masked_array(data = [0 1 2 ..., 4997 4998 4999],
                 mask = [False False False ..., False False False],
           fill_value = 999999)
    ]

We can also query for both the ``a`` and ``b`` fields together::

    >>> with Monary("localhost") as m:
    ...     arrays = m.query("test", "coll", {}, ["a", "b"], ["float64", "int32"])
    ...
    >>> arrays
    [masked_array(data = [0.7288538725115359 0.4277338122483343 0.5252409593667835 ...,
     0.36620052182115614 0.2733050910755992 0.16910275584086776],
                 mask = [False False False ..., False False False],
           fill_value = 1e+20)
    , masked_array(data = [0 1 2 ..., 4997 4998 4999],
                 mask = [False False False ..., False False False],
           fill_value = 999999)
    ]

``arrays`` is now an array containing two masked arrays: one for ``a`` and one for
``b``. The indicies of the two masked arrays correspond to the same document: for
example, ``arrays[0][250]`` and ``arrays[1][250]`` correspond to the values of ``a``
and ``b`` in the 250th document::

    >>> with Monary("localhost") as m:
    ...     arrays = m.query("test", "coll", {}, ["a", "b"], ["float64", "int32"])
    ...
    >>> a = arrays[0][250]
    >>> b = arrays[1][250]
    >>> print a, b
    0.653997767251 250

If we return to the mongo shell to check that our document matches, we can run:

.. code-block:: javascript

     > use test
     > db.coll.find({"b":250})
     { "_id" : ObjectId("553e815e5d1bdb50241c0e41"), "a" : 0.6539977672509849, "b" : 250 }


