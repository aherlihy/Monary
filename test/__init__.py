
import unittest

try:
    import unittest2 as unittest
except ImportError:
    pass


import nose
import pymongo


class IntegrationTest(unittest.TestCase):
    """Base class for TestCases that need a connection to MongoDB to pass."""
    @classmethod
    def setUpClass(cls):
        print "CLLING SUPER"
        try:
            with pymongo.MongoClient() as cx:
                cx.drop_database("monary_test")
        except pymongo.errors.ConnectionFailure as ex:
            raise nose.SkipTest("Can't connect to mongod: " + ex.message)
