# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import pymongo

import monary
from test import db_err, unittest


NUM_TEST_RECORDS = 5000
BLOCK_SIZE = 32 * 50


@unittest.skipIf(db_err, db_err)
class TestBlockQuery(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.records = []
        with pymongo.MongoClient("127.0.0.1", 27017) as c:
            c.drop_database("monary_test")
            coll = c.monary_test.test_data
            for i in range(NUM_TEST_RECORDS):
                r = {"_id": i}
                if (i % 2) == 0:
                    r["x"] = 3
                cls.records.append(r)
            coll.insert(cls.records, safe=True)

    @classmethod
    def tearDownClass(cls):
        with pymongo.MongoClient() as c:
            c.drop_database("monary_test")

    def get_monary_connection(self):
        return monary.Monary("127.0.0.1", 27017)

    def get_monary_blocks(self, colname, coltype):
        with self.get_monary_connection() as m:
            for block, in m.block_query("monary_test", "test_data",
                                        {}, [colname], [coltype],
                                        block_size=BLOCK_SIZE, sort="_id"):
                yield block

    def test_count(self):
        total = 0
        for block in self.get_monary_blocks("_id", "int32"):
            total += block.count()
        assert total == NUM_TEST_RECORDS

    def test_masks(self):
        unmasked = 0
        for block in self.get_monary_blocks("x", "int8"):
            unmasked += block.count()
        target_records = int(NUM_TEST_RECORDS / 2)
        assert unmasked == target_records

    def test_sum(self):
        total = 0
        for block in self.get_monary_blocks("_id", "int32"):
            total += block.sum()
        target_sum = NUM_TEST_RECORDS * (NUM_TEST_RECORDS - 1) / 2
        assert total == target_sum
