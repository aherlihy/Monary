# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import pymongo
from nose import SkipTest

import monary

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
    monary_connection = monary.Monary(host="127.0.0.1",
                                      database="admin",
                                      username="monary_test_user",
                                      password="monary_test_pass")
    col, = monary_connection.query("admin", "junk",
                                   {}, ["route"], ["int32"])
    assert col[0] == 66, "test value could not be retrieved"


def test_with_authenticate_from_uri():
    monary_connection = monary.Monary("mongodb://monary_test_user:monary_test_"
                                      "pass@127.0.0.1:27017/admin")
    col, = monary_connection.query("admin", "junk",
                                   {}, ["route"], ["int32"])
    assert col[0] == 66, "test value could not be retrieved"


def test_bad_authenticate():
    try:
        monary_connection = monary.Monary(host="127.0.0.1",
                                          database="admin",
                                          username="monary_test_user",
                                          password="monary_test_wrong_pass")
        monary_connection.count("admin", "junk", {})
        assert False, "authentication should not have succeeded"\
                      "with wrong password"
    except RuntimeError:
        pass  # auth should have failed


def test_reconnection():
    try:
        monary_connection = monary.Monary(host="127.0.0.1",
                                          database="admin",
                                          username="monary_test_user",
                                          password="monary_test_wrong_pass")
        monary_connection.count("admin", "junk", {})
        assert False, "authentication should not have succeeded"\
                      "with wrong password"
    except RuntimeError:
        pass  # auth should have failed
    monary_connection.connect(host="127.0.0.1",
                              database="admin",
                              username="monary_test_user",
                              password="monary_test_pass")
    col, = monary_connection.query("admin", "junk",
                                   {}, ["route"], ["int32"])
    assert col[0] == 66, "test value could not be retrieved"
