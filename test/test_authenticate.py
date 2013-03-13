# Monary - Copyright 2011-2013 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import pymongo
import bson
import monary

def get_pymongo_connection():
    return pymongo.Connection("127.0.0.1")

def get_monary_connection():
    return monary.Monary("127.0.0.1")

def setup():
    connection = get_pymongo_connection()
    db = connection.monary_test
    db.add_user("monary_test_user", "monary_test_pass")
    db.junk.insert({"route": 66})

def teardown():
    connection = get_pymongo_connection()
    connection.drop_database("monary_test")

def test_with_authenticate():
    with get_monary_connection() as monary:
        success = monary.authenticate("monary_test", "monary_test_user", "monary_test_pass")
        assert success, "authentication failed"
        [ col ] = monary.query("monary_test", "junk", {}, ["route"], ["int32"])
        assert col[0] == 66, "test value could not be retrieved"

def test_bad_authenticate():
    with get_monary_connection() as monary:
        success = monary.authenticate("monary_test", "monary_test_user", "monary_test_wrong_pass")
        assert not success, "authentication should not have succeeded with wrong password"
