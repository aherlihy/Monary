MonaryParam Example
===================

A ``MonaryParam`` represents a single column, i.e. a single field, in a set of
BSON documents. It contains three pieces of data: the name of the field it
represents, the type of the data stored in that field, and the values of the
field itself. For example, say you had a set of 12 documents that all contained
the field "count" with the values 1-12::

    >>> import numpy as np
    >>> count_field = "count"
    >>> count_type = "int64"
    >>> count_values = np.ma.masked_array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])

If you wanted to make a MonaryParam that represented the field ``count``, you could::

   >>> from monary import MonaryParam
   >>> mp = MonaryParam(count_values, count_field, count_type)

Or, because some types can be determined by the type of the NumPy masked array,
you could simply call::

    >>> p = MonaryParam(count_values, count_field)

.. seealso::

    :ref:`The Type section in the Reference <type-reference>`

If you wanted to represent a few different fields, you can create a set of
MonaryParams using lists. Say you have another field, ``month``, in your
set of 12 BSON documents::

    >>> month_field = "month"
    >>> month_type = "string:9"
    >>> month_values = np.ma.masked_array(["january", "february", "march", "april", "may",
    ...                                     "june", "july", "august", "september", "october",
    ...                                     "november", "december"])

You can create multiple MonaryParams using ``from_lists``::

    >>> fields = [count_field, month_field]
    >>> types = [count_type, month_type]
    >>> values = [count_values, month_values]
    >>> params = MonaryParam.from_lists(values, fields, types)
