Monary |release| Documentation
==============================

Overview
--------
**Monary** provides a Python interface for fast column queries from `MongoDB
<http://www.mongodb.org>`_. It is much faster than PyMongo for large
bulk reads from the database into `NumPy <http://www.numpy.org/>`_'s 
`ndarrays <http://docs.scipy.org/doc/numpy/reference/generated/numpy.ndarray.html>`_.

Note that Monary is still in beta. There are no guarantees of API stability;
furthermore, dependencies may change in the future.

Monary is written by `David J. C. Beach <http://djcinnovations.com/>`_.

:doc:`installation`
  Instructions on how to get the distribution.

:doc:`tutorial`
  Getting started quickly with Monary.

:doc:`examples/index`
  Examples of how to perform specific tasks.

:doc:`reference`
  In-depth explanations of how Monary handles BSON types.

:doc:`faq`
  Frequently asked questions about Monary.


Dependencies
------------
Monary depends on the `MongoDB C Driver 1.0 <http://github.com/mongodb/mongo-c-driver>`_,
which does not come bundled. Please install the MongoDB C driver using the
`official instructions. <http://api.mongodb.org/c/current/installing.html>`_

Monary depends on `PyMongo 3.0 <http://api.mongodb.org/python/current/>`_,
`NumPy <http://www.numpy.org/>`_, and `pkgconfig <https://pypi.python.org/pypi/pkgconfig>`_.

Issues
------
All issues can be reported by opening up an issue on the Monary `BitBucket
issues page <https://bitbucket.org/djcbeach/monary/issues>`_.

Contributing
------------
Monary is an open-source project and is hosted on `BitBucket
<https://bitbucket.org/djcbeach/monary/wiki/Home>`_. To contribute, fork the
project and send a pull request. 

We encourage contributors!

See the :doc:`contributors` page for a list of people who have contributed to
developing Monary.

Changes
-------
See the :doc:`changelog` for a full list of changes to Monary.

About This Documentation
------------------------
This documentation is generated using the `Sphinx
<http://sphinx.pocoo.org/>`_ documentation generator. The source files
for the documentation are located in the *doc/* directory of the
Monary distribution.

Indices and tables
------------------
* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. toctree::
   :hidden:
   :maxdepth: 2

   installation
   tutorial
   examples/index
   reference
   faq
   changelog
   contributors
