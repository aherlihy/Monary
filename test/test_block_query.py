# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import pymongo

import monary

NUM_TEST_RECORDS = 5000
BLOCK_SIZE = 32 * 50
records = []


def get_pymongo_connection():
    return pymongo.MongoClient("127.0.0.1", 27017)


def get_monary_connection():
    return monary.Monary("127.0.0.1", 27017)


def setup():
    global records
    with get_pymongo_connection() as c:
        c.drop_database("monary_test")  # ensure that database does not exist
        coll = c.monary_test.test_data
        for i in xrange(NUM_TEST_RECORDS):
            r = {"_id": i}
            if (i % 2) == 0:
                r["x"] = 3
            records.append(r)
        coll.insert(records, safe=True)
        print "setup complete"


def teardown():
    c = get_pymongo_connection()
    c.drop_database("monary_test")
    print "teardown complete"


def get_monary_blocks(colname, coltype):
    with get_monary_connection() as m:
        for block, in m.block_query("monary_test", "test_data",
                                    {}, [colname], [coltype],
                                    block_size=BLOCK_SIZE, sort="_id"):
            yield block


def test_count():
    total = 0
    for block in get_monary_blocks("_id", "int32"):
        total += block.count()
    assert total == NUM_TEST_RECORDS


def test_masks():
    unmasked = 0
    for block in get_monary_blocks("x", "int8"):
        unmasked += block.count()
        print unmasked
    target_records = int(NUM_TEST_RECORDS / 2)
    assert unmasked == target_records


def test_sum():
    total = 0
    for block in get_monary_blocks("_id", "int32"):
        total += block.sum()
        print total
    target_sum = NUM_TEST_RECORDS * (NUM_TEST_RECORDS - 1) / 2
    assert total == target_sum


if __name__ == '__main__':
    setup()
    test_count()
    teardown()
