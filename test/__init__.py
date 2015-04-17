try:
    import unittest2 as unittest
except ImportError:
    import unittest

import pymongo

# In order to skip tests on Python 2.6 must use @unittest.skipIf(...) so tests
# to see if the DB is running need to happen at global scope. Tests will skip
# if the connection error message is "connection closed", and error if the
# message is "connection refused".
db_err = ""
try:
    with pymongo.MongoClient() as cx:
        cx.drop_database("monary_test")
except pymongo.errors.ConnectionFailure as ex:
    if "connection closed" in str(ex):
        db_err = ("Cannot connect to mongod (maybe SSL is turned on?): " +
                  str(ex))
    else:
        raise RuntimeError("Cannot connect to mongod: " + str(ex))
