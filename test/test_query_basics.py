# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import pymongo

import monary
from test import db_err, unittest

NUM_TEST_RECORDS = 5000


@unittest.skipIf(db_err, db_err)
class TestQueryBasics(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with pymongo.MongoClient() as c:
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

    @classmethod
    def tearDownClass(cls):
        with pymongo.MongoClient() as c:
            c.drop_database("monary_test")

    def get_monary_column(self, colname, coltype):
        with monary.Monary("127.0.0.1") as m:
            return m.query("monary_test", "test_data", {}, [colname],
                           [coltype], sort="_id")[0]

    def test_count(self):
        with monary.Monary("127.0.0.1") as m:
            assert m.count("monary_test", "test_data", {}) == NUM_TEST_RECORDS

    def test_masks(self):
        vals = self.get_monary_column("x", "int8")
        target_records = int(NUM_TEST_RECORDS / 2)
        assert vals.count() == target_records

    def test_sum(self):
        vals = self.get_monary_column("_id", "int32")
        target_sum = NUM_TEST_RECORDS * (NUM_TEST_RECORDS - 1) / 2
        assert vals.sum() == target_sum

    def test_sort(self):
        vals = self.get_monary_column("_id", "int32")
        assert (vals == list(range(NUM_TEST_RECORDS))).all()
