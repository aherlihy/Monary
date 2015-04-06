# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import nose
import pymongo

import monary

NUM_TEST_RECORDS = 5000


try:
    with pymongo.MongoClient() as cx:
        cx.drop_database("monary_test")
except pymongo.errors.ConnectionFailure as ex:
    raise nose.SkipTest("Unable to connect to mongod: ", str(ex))


def get_pymongo_connection():
    return pymongo.MongoClient()


def get_monary_connection():
    return monary.Monary("127.0.0.1")


def setup():
    with get_pymongo_connection() as c:
        # Ensure that database does not exist.
        c.drop_database("monary_test")
        db = c.monary_test
        coll = db.test_data
        records = []
        for i in range(NUM_TEST_RECORDS):
            r = {"_id": i}
            if (i % 2) == 0:
                r['x'] = 3
            records.append(r)
        coll.insert(records, safe=True)
        print("setup complete")


def teardown():
    c = get_pymongo_connection()
    c.drop_database("monary_test")
    print("teardown complete")


def get_monary_column(colname, coltype):
    with get_monary_connection() as m:
        return m.query("monary_test", "test_data", {}, [colname],
                       [coltype], sort="_id")[0]


def test_count():
    with get_monary_connection() as m:
        assert m.count("monary_test", "test_data", {}) == NUM_TEST_RECORDS


def test_masks():
    vals = get_monary_column("x", "int8")
    target_records = int(NUM_TEST_RECORDS / 2)
    assert vals.count() == target_records


def test_sum():
    vals = get_monary_column("_id", "int32")
    target_sum = NUM_TEST_RECORDS * (NUM_TEST_RECORDS - 1) / 2
    assert vals.sum() == target_sum


def test_sort():
    vals = get_monary_column("_id", "int32")
    assert (vals == list(range(NUM_TEST_RECORDS))).all()
