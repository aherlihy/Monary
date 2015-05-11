# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import numpy as np

import monary
from test import db_err, unittest


NUM_TEST_RECORDS = 5000
BLOCK_SIZE = 32 * 50


@unittest.skipIf(db_err, db_err)
class TestBlockQuery(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with monary.Monary() as m:
            m.drop_collection("monary_test", "data")
        ids = np.ma.masked_array(np.zeros(NUM_TEST_RECORDS // 2),
                                 np.zeros(NUM_TEST_RECORDS // 2), "int32")
        ids_x = np.ma.copy(ids)
        x = np.ma.masked_array([3] * (NUM_TEST_RECORDS // 2),
                               np.zeros(NUM_TEST_RECORDS // 2),
                               "int32")
        c = 0
        for i in range(NUM_TEST_RECORDS):
            if i % 2 == 0:
                ids_x[c] = i
            else:
                ids[c] = i
                c += 1
        x_param = monary.MonaryParam.from_lists([ids_x, x],
                                                ["_id", "x"],
                                                ["int32", "int32"])
        param = monary.MonaryParam.from_lists([ids], ["_id"], ["int32"])

        with monary.Monary() as m:
            m.insert("monary_test", "data", x_param)
            m.insert("monary_test", "data", param)

    @classmethod
    def tearDownClass(cls):
        with monary.Monary() as m:
            m.drop_collection("monary_test", "data")

    def get_monary_connection(self):
        return monary.Monary("127.0.0.1", 27017)

    def get_monary_blocks(self, colname, coltype):
        with self.get_monary_connection() as m:
            for block, in m.block_query("monary_test", "data",
                                        {}, [colname], [coltype],
                                        block_size=BLOCK_SIZE, sort="_id"):
                yield block

    def test_count(self):
        total = 0
        for block in self.get_monary_blocks("_id", "int32"):
            total += block.count()
        self.assertEqual(total, NUM_TEST_RECORDS)

    def test_masks(self):
        unmasked = 0
        for block in self.get_monary_blocks("x", "int8"):
            unmasked += block.count()
        target_records = int(NUM_TEST_RECORDS / 2)
        self.assertEqual(unmasked, target_records)

    def test_sum(self):
        total = 0
        for block in self.get_monary_blocks("_id", "int32"):
            total += block.sum()
        target_sum = NUM_TEST_RECORDS * (NUM_TEST_RECORDS - 1) / 2
        self.assertEqual(total, target_sum)
