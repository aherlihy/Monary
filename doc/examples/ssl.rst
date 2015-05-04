SSL Example
===========

Running ``mongod`` with SSL
---------------------------
In order to run monary with SSL, mongod needs to be compiled with ssl enabled.
Instructions can be found `here <http://docs.mongodb.org/manual/tutorial/configure-ssl>`_.

To run ``mongod`` with SSL options:

.. code-block:: bash

    $ mongod --sslOnNormalPorts --sslPEMKeyFile <pem> --sslCAFile=<ca>
             --sslWeakCertificateValidation

The test permissions files can be found in the "test/certificates" directory of the Monary source.
To run mongod with the SSL permissions provided by the monary source directory:

.. code-block:: bash

    $ mongod --sslOnNormalPorts --sslPEMKeyFile=test/certificates/server.pem
             --sslCAFile=test/certificates/ca.pem --sslWeakCertificateValidation

First we're going to put some sample data into our DB. Make sure you are using a ``mongo`` shell
that has been compiled with SSL:

.. code-block:: bash

    $ mongo --ssl
      > for (var i = 0; i < 5; i++) { db.ssl.insert({x1:NumberInt(i) }) }
      WriteResult({ "nInserted" : 1 })


The sample certificate files can be downloaded from
`here <https://bitbucket.org/djcbeach/monary/src/b56af115c882ba6b12f426e9a7226e07fccaf77c/test/certificates/?at=default>`_.
Collect the certificate files to be passed to Monary::

     >>> import os
     >>> import monary
     >>> cert_path = os.path.join("test", "certificates")
     >>> client_pem = os.path.join(cert_path, 'client.pem')
     >>> ca_pem = os.path.join(cert_path, 'ca.pem')

Connect with Monary::

     >>> with monary.Monary("mongodb://localhost:27017/?ssl=true",
     ...                    pem_file=client_pem,
     ...                    ca_file=ca_pem,
     ...                    ca_dir=cert_path,
     ...                    weak_cert_validation=False) as m:
     ...     arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
     >>> arrays
     [masked_array(data = [1.0 0.0 1.0 2.0 3.0 4.0],
             mask = [False False False False False False],
       fill_value = 1e+20)
     ]


Password Protected PEM Files
----------------------------
To run ``mongod`` with a password-protected PEM file, you need to run mongo
with the following options::

    $ mongod --sslMode requireSSL --sslPEMKeyFile=<password-protected-pem> --sslPEMKeyPassword "qwerty"

For example, with the test/certificates files::

    $ mongod --sslMode requireSSL --sslPEMKeyFile=test/certificates/password_protected.pem --sslPEMKeyPassword "qwerty"

To connect with an SSL-protected MongoDB instance with Monary, you just need to specify the SSL parameters::

     >>> pwd_pem = os.path.join(cert_path, 'password_protected.pem')
     >>> with monary.Monary("mongodb://localhost:27017/?ssl=true",
     ...                    pem_file=pwd_pem,
     ...                    pem_pwd='qwerty',
     ...                    ca_file=ca_pem,
     ...                    weak_cert_validation=True) as monary:
     ...    arrays = monary.query("test", "ssl", {}, ["x1"], ["float64"])
     >>> arrays
     [masked_array(data = [0.0 1.0 2.0 3.0 4.0],
             mask = [False False False False False],
       fill_value = 1e+20)
     ]

Alternatively, if you do not want to specify the password directly, you can connect without the
``pem_pwd`` parameter. You will be prompted for the password.