Strings Example
===============

This example demonstrates how to extract strings with Monary.

.. note::

    Due to the large difference between Python 2 strings and Python 3 strings,
    some of the examples will have two different versions. Please pay attention
    to the comments to decide which lines would work in your version of Python.

Setup
-----
We can use PyMongo to populate a collection with some test data containing
random strings. First, make a connection::

    >>> from monary import Monary
    >>> client = Monary()

Then, we can create a NumPy masked array of random strings::

    >>> import random
    >>> import string
    >>> from numpy import ma
    >>> import numpy as np
    >>> lowercase = string.ascii_lowercase
    >>> def rand_str(length):
    ...     return "".join(random.choice(lowercase)
    ...                    for c in range(0, length))
    ...
    >>> strs = ma.masked_array(np.zeros(1000, dtype="S10"),
    ...                        np.zeros(1000, dtype="bool"))
    >>> for i in range(0, 1000):
    ...     strs[i] = rand_str(random.choice(range(1, 10))).encode("utf-8")

Finally we can use Monary to insert these strings::

    >>> from monary import MonaryParam
    >>> client.insert(
    ...     "test", "data", [MonaryParam(strs, "stringdata", "string:10")])

.. seealso::

    :doc:`The MonaryParam Example </examples/monary-param>` and
    :doc:`The Monary Insert Example </examples/insert>`


MongoDB encodes strings in UTF-8, so you can use non-ASCII characters. Here is
a sample script that inserts non-ASCII strings:

.. code-block:: python

    # -*- coding: utf-8 -*-
    # the above comment is needed for Python 2 files with non-ASCII characters
    from monary import Monary, MonaryParam
    import numpy as np
    from numpy import ma

    client = Monary()
    strs = ["liberté", "自由", "ελευθερία"]

    # If running in python 2, encode in utf-8
    strs = list(map(lambda x: x.encode("utf-8"), strs))

    max_len = max(map(len, strs))
    str_array = ma.masked_array(strs, [False] * 3, dtype=("S%d" % max_len))
    str_mp = MonaryParam(str_array, "stringdata", "string:%d" % max_len)
    client.insert("test", "utf8", [str_mp])


Finding String Data
-------------------

String Sizing
.............
Monary can obtain strings from MongoDB if you specify the size of the strings
in bytes.
    
.. note:: 

    Unicode characters encoded in UTF-8 can be larger than one byte in size.
    For example, the size of "99¢" is 4 - one byte for each '9' and two bytes
    for '¢'.

It is safe to specify larger sizes; if the length of a string is smaller than
the specified size, the extra space is padded with ``NUL`` characters.

If the length of a string is greater than the specified size, **it will be
truncated**. For example, storing "hello" in a block of size 3 results in
"hel". Multi-byte UTF-8-encoded characters may also be truncated, which may
result in an invalid string.

To determine the length of a Unicode (or ASCII) string in Python, encode the
desired string object into UTF-8 and take its length::

    >>> def strlen_in_bytes(string):
    ...     return len(string.encode("utf-8"))

Monary provides a way to query the database directly for the byte sizes of
strings.

Performing Queries
..................
If we don't know the maximum size of the strings in advance, we can
:doc:`query </examples/query>` for their size, which returns the size of the
strings in bytes::

    >>> from monary import Monary
    >>> client = Monary()
    >>> sizes, = client.query("test", "data", {}, ["stringdata"], ["size"])
    >>> sizes
    masked_array(data = [9L 7L 3L ...,, 6L 5L 9L],
                 mask = [False False False ... False False False],
           fill_value = 999999)

Now that we have the sizes of all the strings, we can find the maximum string
size::

    >>> max_size = sizes.max()

Finally, we can use this size to obtain the actual strings from MongoDB::

    >>> data, = client.query("test", "data", {}, ["stringdata"],
    ...                      ["string:%d" % max_size])
    >>> data
    masked_array(data = ['nbuvggamk' 'bkhwkwl' 'tvb' ..., 'rsdefd' 'lpasx' 'wpdlxierd'],
                 mask = [False False False ..., False False False],
           fill_value = N/A)

Each of these values is a ``numpy.string_`` instance. You can convert it to a
regular Python string if you'd like::

    >>> mystr = str(data[0])  # Python 2
    >>> mystr = data[0].decode("utf-8")  # Python 3

If you have non-ASCII UTF-8 characters in this data, you can create a Unicode
(Python 2) or Str (Python 3) object by decoding the data::

    >>> sizes, = client.query("test", "utf8", {}, ["stringdata"], ["size"])
    >>> data, = client.query("test", "utf8", {}, ["stringdata"],
    ...                      ["string:%d" % sizes.max()])

    >>> # Python 2:
    >>> mystr = unicode(data[0], "utf-8")
    >>> mystr
    u'libert\xe9'
    >>> print mystr
    liberté

    >>> # Python 3:
    >>> mystr = data[0].decode("utf-8")  # Python 3
    >>> mystr
    'liberté'
    >>> print(mystr)
    liberté
