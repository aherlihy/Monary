# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import numpy
import pymongo

import monary
from test import db_err, unittest

NUM_TEST_RECORDS = 5000


@unittest.skipIf(db_err, db_err)
class TestAggregation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with pymongo.MongoClient() as c:
            c.drop_database("monary_test")
            for i in range(NUM_TEST_RECORDS):
                if i % 2 == 0:
                    doc = {
                        "_id": i,
                        "a": 0,
                    }
                else:
                    doc = {
                        "_id": i,
                        "b": 1,
                    }
                doc["data"] = i % 3
                c.monary_test.data.insert(doc)

    @classmethod
    def tearDownClass(cls):
        with pymongo.MongoClient() as c:
            c.drop_database("monary_test")

    def aggregate_monary_column(self, colname, coltype, pipeline, **kwargs):
        with monary.Monary("127.0.0.1") as m:
            result, = m.aggregate("monary_test", "data", pipeline, [colname],
                                  [coltype], **kwargs)
            return result

    def test_group(self):
        pipeline = [{"$group": {"_id": "$data"}}, {"$sort": {"_id": 1}}]
        result = self.aggregate_monary_column("_id", "int32", pipeline)
        expected = numpy.array([0, 1, 2])
        assert (expected == result).all()

    def test_project(self):
        pipeline = [{"$project": {"b": 1, "_id": 0}}]
        result = self.aggregate_monary_column("b", "int32", pipeline)
        assert numpy.count_nonzero(result.mask) == NUM_TEST_RECORDS / 2
        assert result.sum() == NUM_TEST_RECORDS / 2
