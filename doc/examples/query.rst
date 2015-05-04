Query Example
=============

This example shows you how to use Monary's ``query`` method.

Setup
-----
For this example, let's use Monary to insert documents with numerical data
into MongoDB. First, we can set up a connection to the local MongoDB database::

    >>> from monary import Monary
    >>> client = Monary()

Next, we generate some documents. These documents will represent financial
assets::

    >>> import numpy as np
    >>> from numpy import ma
    >>> records = 10000
    >>> unmasked = np.zeros(records, dtype="bool")

    >>> # All of our assets have been sold.
    >>> sold = ma.masked_array(np.ones(records, dtype="bool"), unmasked)

    >>> # The price at which the assets were purchased.
    >>> buy_price = ma.masked_array(np.random.uniform(50, 300, records),
    ...                             np.copy(unmasked))

    >>> delta = np.random.uniform(-10, 30, records)
    >>> # The price at which the assets were sold.
    >>> sell_price = ma.masked_array(buy_price.data + delta, np.copy(unmasked))

Finally, we use Monary to insert the data into MongoDB::

    >>> from monary import MonaryParam
    >>> sold, buy_price, sell_price = MonaryParam.from_lists(
    ...     [sold, buy_price, sell_price],
    ...     ["sold", "price.bought", "price.sold"])

    >>> client.insert(
    ...     "finance", "assets", [sold, buy_price, sell_price])

.. seealso::

    :doc:`The MonaryParam Example </examples/monary-param>` and
    :doc:`The Monary Insert Example </examples/insert>`

Using Query
-----------
Now we query the database, specifying the keys we want from the MongoDB
documents and what type we want the returned data to be::

    >>> buy_price, sell_price = client.query(
    ...     "finance", "assets", {"sold": True},
    ...     ["price.bought", "price.sold"],
    ...     ["float64", "float64"])
    >>> assets_count = sell_price.count()
    >>> gain = sell_price - buy_price   # vector subtraction
    >>> cumulative_gain = gain.sum()

Finally, we can review our financial data::

    >>> cumulative_gain
    100254.10514435501
    >>> assets_count
    10000
