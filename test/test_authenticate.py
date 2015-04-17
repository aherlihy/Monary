# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import pymongo

import monary
from test import db_err, unittest

# If there was no error connecting, need to check if auth is working.
db_auth = db_err
if not db_auth:
    with pymongo.MongoClient() as global_cxn:
        global_opts = global_cxn.admin.command('getCmdLineOpts')['argv']
        if "--auth" not in global_opts:
            db_auth = "The mongo server (mongod) needs to be running " \
                      "with authentication (--auth)"


@unittest.skipIf(db_auth, db_auth)
class TestAuthentication(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        connection = pymongo.MongoClient()
        cls.db = connection.admin
        cls.db.add_user("monary_test_user", "monary_test_pass", roles=["root"])
        cls.db.authenticate("monary_test_user", "monary_test_pass")
        cls.db.junk.insert({"route": 66})

    @classmethod
    def tearDownClass(cls):
        cls.db.junk.drop()
        cls.db.remove_user("monary_test_user")

    def test_with_authenticate(self):
        with monary.Monary(host="127.0.0.1",
                           database="admin",
                           username="monary_test_user",
                           password="monary_test_pass") as m:
            col, = m.query("admin", "junk",
                           {}, ["route"], ["int32"])
            assert col[0] == 66, "test value could not be retrieved"

    def test_with_authenticate_from_uri(self):
        with monary.Monary("mongodb://monary_test_user:monary_test_"
                           "pass@127.0.0.1:27017/admin") as m:
            col, = m.query("admin", "junk",
                           {}, ["route"], ["int32"])
            assert col[0] == 66, "test value could not be retrieved"

    def test_bad_authenticate(self):
        with self.assertRaisesRegexp(monary.monary.MonaryError,
                                     "Failed to authenticate credentials"):
            with monary.Monary(host="127.0.0.1",
                               database="admin",
                               username="monary_test_user",
                               password="monary_test_wrong_pass") as m:
                m.count("admin", "junk", {})

    def test_reconnection(self):
        with monary.Monary(host="127.0.0.1",
                           database="admin",
                           username="monary_test_user",
                           password="monary_test_wrong_pass") as m:
            with self.assertRaisesRegexp(monary.monary.MonaryError,
                                         "Failed to authenticate"
                                         " credentials"):
                m.count("admin", "junk", {})

            m.connect(host="127.0.0.1",
                      database="admin",
                      username="monary_test_user",
                      password="monary_test_pass")
            col, = m.query("admin", "junk",
                           {}, ["route"], ["int32"])
            assert col[0] == 66, "test value could not be retrieved"
