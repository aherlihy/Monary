.. _type-reference:

Type Reference
==============
Monary converts values between BSON and NumPy. The following data types can be
stored in NumPy arrays:

 * ``id``: `ObjectID <http://dochub.mongodb.org/core/objectids>`_
 * ``bool``: boolean
 * ``int8``: signed two's compliment 8-bit integer
 * ``int16``: signed two's compliment 16-bit integer
 * ``int32``: signed two's compliment 32-bit integer
 * ``int64``: signed two's compliment 64-bit integer
 * ``uint8``: unsigned 8-bit integer
 * ``uint16``: unsigned 16-bit integer
 * ``uint32``: unsigned 32-bit integer
 * ``uint64``: unsigned 64-bit integer
 * ``float32``: IEEE 754 single-precision (32-bit) floating point value 
 * ``float64``: IEEE 754 single-precision (64-bit) floating point value
 * ``date``: UTC datetime
 * ``timestamp``:
   `Timestamp <http://docs.mongodb.org/manual/reference/bson-types/#timestamps>`_
 * ``string``: UTF-8 string
 * ``binary``: binary data
 * ``bson``: BSON document
 * ``type``: <see below>
 * ``size``: <see below>
 * ``length``: <see below>

When values are retrieved from MongoDB they are converted from BSON types to
the NumPy types you specify. See query, block_query, and aggregate. All types
are implemented in C, thus type conversions follow the rules of the C standard.

When values are inserted into MongoDB they are converted from NumPy types to
BSON types. In most cases the BSON type can be inferred from the input NumPy
type, except for types ``id``, ``date``, ``timestamp``, ``string``, ``binary``,
and ``bson``.



.. seealso::

    The official `BSON Specification <http://bsonspec.org/spec.html>`_ for more
    information about how BSON is stored in binary format.

BSON Types
----------
Integers
........
`BSON <http://bsonspec.org/>`_ only stores signed 32- and 64-bit integers.
Specifying an unsigned integer or an integer size of 8- or 16-bits causes a
cast. Casting a negative number to an unsigned integer or casting to a smaller
integer size with overflow is implementation-defined, depending on the C
compiler for your platform.

Floating-point numbers can be cast to integers. In the case of overflow, the
result is undefined.

Note that signed integers are kept in two's complement format.

Floating-Point
..............
BSON only stores doubles; that is, 64-bit IEEE 754 floating point
numbers. Specifying float32 will cause a cast with possible loss of precision.

Integers can be cast to floating-point. If the original value is outside of the
range of the destination type, the result is undefined.

Datetimes
.........
This datetime is a 64-bit integer representing milliseconds since the epoch,
which is January 1, 1970. Dates before the epoch are expressed as negative
milliseconds. Monary provides helper functions for converting MongoDB dates
into Python datetime objects.

Timestamps
..........
A BSON timestamp is a special type used internally by MongoDB. It is a 64-bit
integer, where the first four bytes represent an increment and the second four
represent a timestamp. For storing arbitrary times, use datetime instead.

.. seealso::

    :doc:`examples/timestamp` for an example of using timestamps.

Binary
......
Binary data retrieved from MongoDB is accessed via 

.. seealso::

    :doc:`examples/binary` for an example of using binary data.

Strings
.......
All strings in MongoDB are encoded in UTF-8. When performing a find query on
strings, you must also input the lengths of the strings in bytes. For a regular
ASCII string, the length is the number of characters. Characters with
higher-order UTF-8 encodings may occupy more space. You can use Monary to query
for the strings' actual size in bytes to determine what size to use.

Find queries return lists of ``numpy.string_`` objects.

.. seealso::

    :doc:`examples/string` for an example of using strings.

Subdocuments
............
Documents are retrieved as BSON. Each value is a NumPy void pointer to the
binary data.

Monary-Specific Types
---------------------
Type
....
"Type" refers to a field's BSON type code. For integers, the type code returned
will be either an int32 (type code 16) or int64 (type code 18), depending on
how it is stored in MongoDB.

Here is a list of selected type codes, as per the specification:

-  1 : double
-  2 : string
-  3 : (sub)document
-  4 : array
-  5 : binary
-  7 : ObjectID
-  8 : boolean
-  9 : UTC datetime
- 16 : 32-bit integer
- 17 : timestamp
- 18 : 64-bit integer

.. seealso::

    :ref:`integer-double-type-code`

Size
....
For UTF-8 strings, JavaScript code, binary values, BSON subdocuments, and
arrays, "size" is defined as the size of the object in bytes. All other types
do not have a defined Monary size.

Length
......
For ASCII/UTF-8 strings and Javascript code, "length" refers to the string
length (the same as ``len`` on a string); for arrays, the number of elements;
and for documents, the number of key-value pairs. No other types have a defined
Monary length.


.. _write-concern-reference:

Write Concern Reference
=======================
The Monary WriteConcern object allows users to specify the type of write Monary
will perform. This object will be converted into a C struct and used for
``insert``, ``remove``, and ``update``. The parameters to the constructor mimic
the MongoDB Write Concern options.

.. seealso::
    `The MongoDB manual entry on Write Concern
    <http://docs.mongodb.org/manual/reference/write-concern/>`_


wtimeout
--------
This option specifies a time limit, in milliseconds, for the write concern.
``wtimeout`` is only applicable for ``w`` values greater than 1.

``wtimeout`` causes write operations to return with an error after the
specified limit, even if the required write concern is not fulfilled. When
these write operations return, MongoDB does not undo successful data
modifications performed before the write concern exceeded the wtimeout time
limit.

If you do not specify the ``wtimeout`` option and the level of write concern is
unachievable, the write operation will block indefinitely. Specifying a
``wtimeout`` value of 0 is equivalent to a write concern without the
``wtimeout`` option.

wjournal
--------
The ``wjournal`` option confirms that the mongod instance has written the data
to the on-disk journal. This ensures that data is not lost if the mongod
instance shuts down unexpectedly. Set to ``True`` to enable.

wtag
----
By specifying a ``wtag``, you can have fine-grained control over which replica
set members must acknowledge a write operation to satisfy the required level of
write concern.

.. seealso::

    `The MongoDB tag set configuration tutorial
    <http://docs.mongodb.org/manual/tutorial/configure-replica-set-tag-sets/#replica-set-configuration-tag-sets>`_
