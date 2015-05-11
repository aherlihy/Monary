# -*- coding: utf-8 -*-
# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import sys

import numpy as np
import pymongo

import monary
from test import db_err, unittest


@unittest.skipIf(db_err, db_err)
class TestUTF8(unittest.TestCase):
    expected = ["aあ", "âéÇ", "αλΩ", "çœ¥¨≠"]

    @classmethod
    def setUpClass(cls):
        with pymongo.MongoClient() as c:
            c.drop_database("monary_test")

        if sys.version_info[0] >= 3:
            # Python 2: convert from str to unicode.
            cls.expected = [s.encode('utf-8') for s in cls.expected]
        strs = np.ma.masked_array(cls.expected, np.zeros(4), "S12")
        seq = np.ma.masked_array([0, 1, 2, 3], np.zeros(4), "int32")

        strs_param = monary.MonaryParam.from_lists([strs, seq],
                                                   ["test", "sequence"],
                                                   ["string:12", "int32"])
        with monary.Monary() as m:
            m.insert("monary_test", "data2", strs_param)

    @classmethod
    def tearDownClass(cls):
        with pymongo.MongoClient() as c:
            c.drop_database("monary_test")

    def test_utf8(self):
        with monary.Monary("127.0.0.1") as m:
            data, lens, sizes = m.query(
                "monary_test", "data", {}, ["test", "test", "test"],
                ["string:12", "length", "size"], sort="sequence")
            self.assertTrue((lens < sizes).all())

        for monary_bytes, monary_len, expected_str, in zip(data,
                                                           lens,
                                                           self.expected):
            monary_str = monary_bytes.decode('utf8')

            # We got the same string out from Monary as we inserted w/ PyMongo.
            self.assertEqual(monary_str, expected_str)

            # Monary's idea of "length" == len(string).
            self.assertEqual(monary_len, len(expected_str))
