Installing / Upgrading
======================
.. highlight:: bash

**Monary** is in the `Python Package Index
<http://pypi.python.org/pypi/Monary>`_.


Installing from Source
----------------------
You can install Monary from source, which provides the latest features (but
may be unstable). Simply clone the repository and execute the installation
command::

    $ hg clone https://bitbucket.org/djcbeach/monary
    $ cd monary
    $ python setup.py install

Before you install Monary, you need to have the MongoDB C Driver installed.

Installing CMongo
-----------------
Monary requires the MongoDB C Driver. To install the C driver, you can
use a package manager or follow the instructions in the official
`MongoDB C Driver 1.0 Github <http://github.com/mongodb/mongo-c-driver>`_.
The MongoDB C Driver (libmongoc) uses the Bson library (libbson) which comes bundled.

.. note::
   **LIBMONGOC MUST BE COMPILED WITH SSL AND SASL.**


Monary uses `pkgconfig <https://pypi.python.org/pypi/pkgconfig/>`_ to find the
libmongoc and libbson installations. If pkgconfig cannot find the libraries, it will
look in the default locations: C:\\Program Files\\libmongoc and
C:\\Program Files\\libbson for Windows, and /usr/local for other systems. If you cannot
use pkgconfig **and** libmongoc and libbson are not installed in the default directories,
you will need to pass the locations to the installation script::

   $ python setup.py install --default-libmongoc C:\\usr --default-libbson C:\\usr

If you are installing via ``pip``, and libcmongo and libbson are not installed in the
default directories, you must pass ``--default-libmongoc`` and ``--default-libbson``
to pip using ``--install-option``.

.. note::
   Monary assumes that libmongoc is installed so that the libraries are in
   **<default-libmongoc>/lib**. It also expects that the headers are located
   under **<default-libmongoc>/include**.
   This is also true for libbson.

Installing with pip
-------------------

You can use `pip <http://pypi.python.org/pypi/pip>`_ to install monary in
platforms other than Windows::

    $ pip install monary

To get a specific version of monary::

    $ pip install monary==0.4.0

To upgrade using pip::

    $ pip install --upgrade monary
    
To specify the location of libcmongo and libbson::

   $ pip install --install-option="--default-libmongoc" --install-option="C:\\usr"
     --install-option="--default-libbson" --install-option="C:\\usr" monary


.. note::
    Although Monary provides a Python package in .egg format, pip does not
    support installing from Python eggs. If you would like to install Monary
    with a .egg provided on PyPI, use easy_install instead.

Installing with easy_install
----------------------------

To use ``easy_install`` from `setuptools
<http://pypi.python.org/pypi/setuptools>`_ do:

.. code-block:: bash

    $ easy_install monary

To upgrade:

.. code-block:: bash

    $ easy_install -U monary

Installing on Windows
---------------------
Monary provides Python eggs pre-compiled for Windows distributions. First you
must `install easy_install
<http://simpledeveloper.com/how-to-install-easy_install/>`_. To install Monary,
download `the latest monary egg
<https://testpypi.python.org/packages/2.7/M/Monary/Monary-0.4.0-py2.7.egg>`_.
Then from the command line, ``cd`` to where the egg file was downloaded and
type:

.. code-block:: bash

    $ easy_install Monary-0.4.0-py2.7.egg
 
If you are planning on doing development on Windows or want to build it from
source, there are instructions in windows_instructions.txt, which can be found
in the source distribution. These have been tested on 64-bit Windows using 
Visual Studio 2013.


Installing on OSX
-----------------
Monary provides Python wheels that can be installed directly on OSX.

Installing on Other Unix Distributions
--------------------------------------
Monary uses the `MongoDB C driver <https://github.com/mongodb/mongo-c-driver>`_.
If you install Monary on Linux, BSD and Solaris, you'll need to be able to
compile the C driver with the GNU C compiler.

