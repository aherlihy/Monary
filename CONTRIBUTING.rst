Contributing to Monary
=======================
`source <https://bitbucket.org/djcbeach/monary/wiki/Home>`_
`docs <http://cereal.rutgers.edu/~kds124/monary/index.html>`_

Bugfixes and New Features
-------------------------

The bug tracker is located on bitbucket: `issues
<https://bitbucket.org/djcbeach/monary/issues?status=new&status=open>`_
and `pull requests<https://bitbucket.org/djcbeach/monary/pull-requests>`_.

Supported Interpreters
----------------------

Monary supports python 2.6, 2.7, and 3.3+

Exception Handling
-------------------

Monary should be throwing specific exceptions when errors occur. If the error
is a mongo or monary error, monary.MonaryError (defined in monary.py) should 
be thrown with either a message describing what went wrong or the error
string that was passed from C. 

For new functions in cmonary.c that are called from monary.py, an bson_error_t* 
argument should be added that can be populated in the case of failure. Additionally,
modifications to existing cmonary functions should make sure to fill the error 
argument in case of failure so the user will know exactly what went wrong and where.


