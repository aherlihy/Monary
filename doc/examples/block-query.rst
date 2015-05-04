Block Query
===========

This example demonstrates the use of Monary's ``block_query`` command.

``block_query`` functions similarly to ``query``. The main difference is that
``block_query`` returns a generator. Furthermore, all but the last NumPy masked
arrays that block_query returns will be overwritten as you iterate
through the results. This allows you to process unlimited or unknown amounts
of data with a fixed amount of memory.

Setup
-----
This setup will be identical to the setup in
:doc:`the query example </examples/query>`.

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

Using Block Query
-----------------
Now we query the database, specifying also how many results we want per block::

    >>> cumulative_gain = 0.0
    >>> assets_count = 0
    >>> for buy_price_block, sell_price_block in (
    ...     client.block_query("finance", "assets", {"sold": True},
    ...                        ["price.bought", "price.sold"],
    ...                        ["float64", "float64"],
    ...                        block_size=1024)):
    ...     assets_count += sell_price_block.count()
    ...     gain = sell_price_block - buy_price_block   # vector subtraction
    ...     cumulative_gain += gain.sum()

Finally, we can review our financial data::

    >>> cumulative_gain
    100254.10514435501
    >>> assets_count
    10000
