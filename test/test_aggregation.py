# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import numpy as np

import monary
from test import db_err, unittest

NUM_TEST_RECORDS = 5000


@unittest.skipIf(db_err, db_err)
class TestAggregation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with monary.Monary() as m:
            m.dropCollection("monary_test", "data")

        a_elem = np.ma.masked_array(np.zeros(NUM_TEST_RECORDS // 2),
                                    np.zeros(NUM_TEST_RECORDS // 2), "int32")
        b_elem = np.ma.masked_array(np.ones(NUM_TEST_RECORDS // 2),
                                    np.zeros(NUM_TEST_RECORDS // 2), "int32")
        a_ids = np.ma.copy(a_elem)
        b_ids = np.ma.copy(a_elem)
        a_data = np.ma.copy(a_elem)
        b_data = np.ma.copy(a_elem)

        c = 0
        for i in range(NUM_TEST_RECORDS):
            if i % 2 == 0:
                a_ids[c] = i
                a_data[c] = i % 3
            else:
                b_ids[c] = i
                b_data[c] = i % 3
                c += 1

        a_param = monary.MonaryParam.from_lists(
            [a_elem, a_ids, a_data],
            ["a", "_id", "data"],
            ["int32", "int32", "int32"])
        b_param = monary.MonaryParam.from_lists(
            [b_elem, b_ids, b_data],
            ["b", "_id", "data"],
            ["int32", "int32", "int32"])

        with monary.Monary() as m:
            m.insert("monary_test", "data", a_param)
            m.insert("monary_test", "data", b_param)

    @classmethod
    def tearDownClass(cls):
        with monary.Monary() as m:
            m.dropCollection("monary_test", "data")

    def aggregate_monary_column(self, colname, coltype, pipeline, **kwargs):
        with monary.Monary("127.0.0.1") as m:
            result, = m.aggregate("monary_test", "data", pipeline, [colname],
                                  [coltype], **kwargs)
            return result

    def test_group(self):
        pipeline = [{"$group": {"_id": "$data"}}, {"$sort": {"_id": 1}}]
        result = self.aggregate_monary_column("_id", "int32", pipeline)
        expected = np.array([0, 1, 2])
        self.assertTrue((expected == result).all())

    def test_project(self):
        pipeline = [{"$project": {"b": 1, "_id": 0}}]
        result = self.aggregate_monary_column("b", "int32", pipeline)
        self.assertEqual(np.count_nonzero(result.mask), NUM_TEST_RECORDS // 2)
        self.assertEqual(result.sum(), NUM_TEST_RECORDS // 2)
