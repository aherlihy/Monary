# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import pymongo

import monary

db = None


def setup():
    global db
    cmd_opts = pymongo.MongoClient().admin.command('getCmdLineOpts')['argv']
    assert "--auth" in cmd_opts, "The mongo server (mongod) needs to be"\
                                 "running with authentication (--auth)"
    connection = pymongo.Connection()
    db = connection.monary_auth_test
    db.add_user("monary_test_user", "monary_test_pass")
    db.authenticate("monary_test_user", "monary_test_pass")
    db.junk.insert({"route": 66})


def teardown():
    global db
    db.junk.drop()
    db.remove_user("monary_test_user")


def test_with_authenticate():
    monary_connection = monary.Monary(host="127.0.0.1",
                                      database="monary_auth_test",
                                      username="monary_test_user",
                                      password="monary_test_pass")
    col, = monary_connection.query("monary_auth_test", "junk",
                                   {}, ["route"], ["int32"])
    assert col[0] == 66, "test value could not be retrieved"


def test_with_authenticate_from_uri():
    monary_connection = monary.Monary("mongodb://monary_test_user:monary_test_"
                                      "pass@127.0.0.1:27017/monary_auth_test")
    col, = monary_connection.query("monary_auth_test", "junk",
                                   {}, ["route"], ["int32"])
    assert col[0] == 66, "test value could not be retrieved"


def test_bad_authenticate():
    try:
        monary_connection = monary.Monary(host="127.0.0.1",
                                          database="monary_auth_test",
                                          username="monary_test_user",
                                          password="monary_test_wrong_pass")
        monary_connection.count("monary_auth_test", "junk", {})
        assert False, "authentication should not have succeeded"\
                      "with wrong password"
    except RuntimeError:
        pass  # auth should have failed


def test_reconnection():
    try:
        monary_connection = monary.Monary(host="127.0.0.1",
                                          database="monary_auth_test",
                                          username="monary_test_user",
                                          password="monary_test_wrong_pass")
        monary_connection.count("monary_auth_test", "junk", {})
        assert False, "authentication should not have succeeded"\
                      "with wrong password"
    except RuntimeError:
        pass  # auth should have failed
    monary_connection.connect(host="127.0.0.1",
                              database="monary_auth_test",
                              username="monary_test_user",
                              password="monary_test_pass")
    col, = monary_connection.query("monary_auth_test", "junk",
                                   {}, ["route"], ["int32"])
    assert col[0] == 66, "test value could not be retrieved"
