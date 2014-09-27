# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import numpy
import pymongo

import monary

NUM_TEST_RECORDS = 5000


def setup():
    with pymongo.MongoClient() as c:
        c.drop_database("monary_test")

        for i in range(NUM_TEST_RECORDS):
            if i % 2 == 0:
                doc = {
                    "_id" : i,
                    "a" : 0,
                }
            else:
                doc = {
                    "_id" : i,
                    "b" : 1,
                }
            doc["data"] = i % 3
            c.monary_test.data.insert(doc)


def teardown():
    with pymongo.MongoClient() as c:
        c.drop_database("monary_test")


def aggregate_monary_column(colname, coltype, pipeline, **kwargs):
    with monary.Monary("127.0.0.1") as m:
        result = None
        for block, in m.block_aggregate("monary_test", "data", pipeline,
                                        [colname], [coltype], **kwargs):
            if result is None:
                result = block
            else:
                result += block
        return result


def test_group():
    pipeline = [{"$group" : {"_id" : "$data"}}, {"$sort" : {"_id" : 1}}]
    result = aggregate_monary_column("_id", "int32", pipeline)
    expected = numpy.array([0, 1, 2])
    assert (expected == result).all()


def test_project():
    pipeline = [{"$project" : {"b" : 1, "_id" : 0}}]
    result = aggregate_monary_column("b", "int32", pipeline)
    assert numpy.count_nonzero(result.mask) == NUM_TEST_RECORDS / 2
    assert result.sum() == NUM_TEST_RECORDS / 2
