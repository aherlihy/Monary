MonaryParam Example
===================

A ``MonaryParam`` is used to store data, the data's associated monary type, and
a corresponding field name. If, for example, you want to insert documents from
the masked array ``vals`` of the type ``int64``, and you wanted these to be the
value associated with the key ``foo``, you would create a ``MonaryParam`` as
follows::

    >>> from monary import MonaryParam
    >>> p = MonaryParam(vals, "foo", "int64")

Or, because some types can be determined by the type of the NumPy masked array,
you could simply call::

    >>> p = MonaryParam(vals, "foo")

.. seealso::

    :ref:`The Type section in the Reference <type-reference>`

If you have a list of masked arrays, and you have a list of what fields you
want them associated with, you can make a list of MonaryParams like this::

    >>> vals = [int_arr, bool_arr, id_arr]
    >>> fields = ["foo", "bar", "baz"]
    >>> types = ["int32", "bool", "id"]
    >>> params = MonaryParam.from_lists(vals, fields, types)
