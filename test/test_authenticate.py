# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import pymongo
from nose import SkipTest

import monary
from test_helpers import assertraises

db = None

try:
    pymongo.MongoClient()
except pymongo.errors.ConnectionFailure as e:
    raise SkipTest("Unable to connect to mongod: ", str(e))


def setup():
    global db
    connection = pymongo.MongoClient()
    db = connection.admin
    db.add_user("monary_test_user", "monary_test_pass", roles=["root"])
    db.authenticate("monary_test_user", "monary_test_pass")
    cmd_opts = db.command('getCmdLineOpts')['argv']
    if "--auth" not in cmd_opts:
        raise SkipTest("The mongo server (mongod) needs to be "
                       "running with authentication (--auth)")
    db.junk.insert({"route": 66})


def teardown():
    db.junk.drop()
    db.remove_user("monary_test_user")


def test_with_authenticate():
    with monary.Monary(host="127.0.0.1",
                       database="admin",
                       username="monary_test_user",
                       password="monary_test_pass") as m:
        col, = m.query("admin", "junk",
                       {}, ["route"], ["int32"])
        assert col[0] == 66, "test value could not be retrieved"


def test_with_authenticate_from_uri():
    with monary.Monary("mongodb://monary_test_user:monary_test_"
                       "pass@127.0.0.1:27017/admin") as m:
        col, = m.query("admin", "junk",
                       {}, ["route"], ["int32"])
        assert col[0] == 66, "test value could not be retrieved"


def test_bad_authenticate():
    with assertraises(monary.monary.MonaryError,
                      "Failed to authenticate credentials"):
        with monary.Monary(host="127.0.0.1",
                           database="admin",
                           username="monary_test_user",
                           password="monary_test_wrong_pass") as m:
            m.count("admin", "junk", {})


def test_reconnection():
    with monary.Monary(host="127.0.0.1",
                       database="admin",
                       username="monary_test_user",
                       password="monary_test_wrong_pass") as m:
        with assertraises(monary.monary.MonaryError,
                          "Failed to authenticate credentials"):
            m.count("admin", "junk", {})

        m.connect(host="127.0.0.1",
                  database="admin",
                  username="monary_test_user",
                  password="monary_test_pass")
        col, = m.query("admin", "junk",
                       {}, ["route"], ["int32"])
        assert col[0] == 66, "test value could not be retrieved"
