Connection Example
------------------
To use Monary, we need to create a connection to a running ``mongod``.
instance. To connect to a MongoDB server, we simply make a new Monary object.
The default host and port are ``"localhost"`` and ``27017`` respectively. This
will connect to the default host and port::

    >>> from monary import Monary
    >>> client = Monary()

However, host and port can be specified explicitly::

    >>> client = Monary("example.database.com", 8123)

More options are available for connection::

    >>> client = Monary("example.database.com", 8123,
    ...                 username="sampleUser",
    ...                 password="1234monary5678",
    ...                 database="sidedishes",
    ...                 options={"replicaSet": "test",
    ...                          "connectTimeoutMS": 12345678})


If you want to connect to a MongoDB instance with SSL, see :doc:`SSL <SSL>`.


Alternatively, you can make a connection by specifying a MongoDB URI strings::

    >>> client = Monary("mongodb://me@password:test.example.net:2500/database?replicaSet=test&connectTimeoutMS=300000")

.. seealso::

    The `MongoDB connection string format
    <http://docs.mongodb.org/manual/reference/connection-string/>`_ for more
    information about how these URI's are formatted.