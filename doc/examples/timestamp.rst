Timestamp Example
=================

This example demonstrates how to extract timestamps with Monary.

Setup
-----
We can use Monary to populate a collection with some test data containing
random timestamps. First, make a connection::

    >>> from monary import Monary
    >>> client = Monary()

Then we can generate random timestamps::

    >>> import random
    >>> import bson
    >>> timestamps = []
    >>> for _ in range(10000):
    ...     time = random.randint(0, 1000000)
    ...     inc = random.randint(0, 1000000)
    ...     ts = bson.timestamp.Timestamp(time=time, inc=inc)
    ...     timestamps.append(ts)

Next we put these values into a numpy masked array::

    >>> import numpy as np
    >>> from numpy import ma
    >>> timestamps = [(ts.time << 32) + ts.inc for ts in timestamps]
    >>> ts_array = ma.masked_array(np.array(timestamps, dtype="uint64"),
    ...                            np.zeros(len(timestamps), dtype="bool"))

Finally we use monary to insert this data into MongoDB::

    >>> from monary import MonaryParam
    >>> client.insert(
    ...     "test", "data", [MonaryParam(ts_array, "ts", "timestamp")])

.. seealso::

    :doc:`The MonaryParam Example </examples/monary-param>` and
    :doc:`The Monary Insert Example </examples/insert>`


Finding Timestamp Data
----------------------

Next we use :doc:`query </examples/query>` to get back our data::

    >>> timestamps, = client.query("test", "data", {},
    ...                            ["ts"], ["timestamp"])

Finally, we use `struct <https://docs.python.org/2/library/struct.html>`_ to
unpack the resulting data::

    >>> import struct
    >>> data = [struct.unpack("<ii", ts) for ts in timestamps]
    >>> timestamps = [bson.timestamp.Timestamp(time=time, inc=inc)
    ...                for time, inc in data]
    >>> timestamps[0]
    Timestamp(870767, 595669)
