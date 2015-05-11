try:
    import unittest2 as unittest
except ImportError:
    import unittest

import monary

# In order to skip tests on Python 2.6 must use @unittest.skipIf(...) so tests
# to see if the DB is running need to happen at global scope. Tests will skip
# if the connection error message is "connection closed", and error if the
# message is "connection refused".
db_err = ""
try:
    with monary.Monary() as m:
        m.drop_collection("monary_test", "data")
except monary.monary.MonaryError as ex:
    if "Failed to read 4 bytes" in str(ex):
        db_err = ("Cannot connect to mongod (maybe SSL is turned on?): " +
                  str(ex))
    else:
        raise RuntimeError("Cannot connect to mongod: " + str(ex))
