Changelog
=========

Changes in Version 0.4.0
------------------------
- Remove vendoring of libmongoc - users **must** install libmongoc 1.0 or later independently.
- Improved error handling and error reporting.
- Improved Testing: tests can be run through ``setup.py test``; added skiptest; removed dependency on Nose.
- Inserts.
- Connection over SSL.
- Various bugfixes.


Changes in Version 0.3.0
------------------------
Version 0.3.0 is a major overhaul of the backend code.

- Upgrade to latest version of the MongoDB C driver (0.98.0).
- ``monary_connect`` now takes a MongoDB URI or hostname and port. See the
  `connection string documentation
  <http://docs.mongodb.org/manual/reference/connection-string/>`_ for more
  information.
- Monary can now freely cast between integer and floating-point values.
- Debug messages are suppressed by default.
- ``datehelper`` now allows negative timedeltas and time values to represent
  dates before the epoch.
- Monary objects no longer support the ``authenticate()`` method, which is a
  breaking change. If your code relied on ``authenticate()``, you must now
  include the username and password in the MongoDB URI passed into the Monary
  constructor.
  Authentication now occurs when a connection is made.


Issues Resolved
...............
The new connection format fixes a `bug
<https://bitbucket.org/djcbeach/monary/issue/5/if-host-is-set-to-localhost-in>`_
where connection failures or invoking the constructor with
``monary.Monary("localhost")`` would cause a segmentation fault.

Changes in Version 0.2.3
------------------------
Bugfix release.

Issues Resolved
...............
Fixed a bug with query sorts.

Changes in Version 0.1.4
------------------------
Upgraded to the latest version of the MongoDB C driver - changed the signature
for the ``mongo_connect()`` function.

Changes in Version 0.1.3
------------------------
Added support for sorting queries and providing hints - see ``Monary.query``.

Added simple unit tests for ``Monary.authenticate``.

Changes in Version 0.1.2
------------------------
Added support for a "date" type which populates an array of in64 values from a
BSON date. The date value is milliseconds since January 1, 1970.

Column tests improved.

Strict argument checks added to datehelper functions.

Issues Resolved
...............
Fixed a minor bug in ``datehelper.mongodelta_to_timedelta()``, which was not
accepting a ``numpy.int64`` instance as the date value. (Now we simply convert
the argument to a Python int.)

Changes in Version 0.1.1
------------------------
Added support for int8, int16, int64 and float32 column types. Also added basic
tests for all column types (requires ``nosetests``).

To run the tests, first obtain ``nosetests``::

    $ pip install nose

Then, to test::

    $ nosetests

Issues Resolved
...............
Fixed issue with ObjectIDs containing NULL bytes. (ObjectIDs now use a 12-byte
'void' array type in numeric Python.)

Changes in Version 0.1.0
------------------------

Initial release.
