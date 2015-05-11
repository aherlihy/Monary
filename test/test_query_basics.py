# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import numpy as np
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
            m.insert("monary_test", "test_data", x_param)
            m.insert("monary_test", "test_data", param)

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
            self.assertEqual(m.count("monary_test", "test_data", {}),
                             NUM_TEST_RECORDS)

    def test_masks(self):
        vals = self.get_monary_column("x", "int8")
        target_records = int(NUM_TEST_RECORDS // 2)
        self.assertEqual(vals.count(), target_records)

    def test_sum(self):
        vals = self.get_monary_column("_id", "int32")
        target_sum = NUM_TEST_RECORDS * (NUM_TEST_RECORDS - 1) // 2
        self.assertEqual(vals.sum(), target_sum)

    def test_sort(self):
        vals = self.get_monary_column("_id", "int32")
        self.assertTrue((vals == list(range(NUM_TEST_RECORDS))).all())
