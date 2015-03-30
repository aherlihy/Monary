
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
        try:
            with pymongo.MongoClient(serverSelectionTimeoutMS=5) as cx:
                cx.admin.command('ismaster')
                cx.drop_database("monary_test")
        except pymongo.errors.ConnectionFailure as ex:
            if "Connection refused" in str(ex):
                raise ex
            raise nose.SkipTest("Can't connect to mongod, may be configured "
                                "with SSL: " + str(ex))
