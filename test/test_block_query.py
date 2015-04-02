# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import pymongo

from test import IntegrationTest

import monary


NUM_TEST_RECORDS = 5000
BLOCK_SIZE = 32 * 50


class TestBlockQuery(IntegrationTest):
    @classmethod
    def setUpClass(cls):
        super(TestBlockQuery, cls).setUpClass()

        cls.records = []
        with pymongo.MongoClient("127.0.0.1", 27017) as c:
            # Ensure that database does not exist.
            c.drop_database("monary_test")
            coll = c.monary_test.test_data
            for i in range(NUM_TEST_RECORDS):
                r = {"_id": i}
                if (i % 2) == 0:
                    r["x"] = 3
                cls.records.append(r)
            coll.insert_many(cls.records)
            print("setup complete")

    @classmethod
    def tearDownClass(cls):
        c = pymongo.MongoClient("127.0.0.1", 27017)
        c.drop_database("monary_test")
        print("teardown complete")

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
            print(unmasked)
        target_records = int(NUM_TEST_RECORDS / 2)
        assert unmasked == target_records

    def test_sum(self):
        total = 0
        for block in self.get_monary_blocks("_id", "int32"):
            total += block.sum()
            print(total)
        target_sum = NUM_TEST_RECORDS * (NUM_TEST_RECORDS - 1) / 2
        assert total == target_sum
