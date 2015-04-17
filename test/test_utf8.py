# -*- coding: utf-8 -*-
# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import sys

import pymongo

import monary
from test import db_err, unittest


@unittest.skipIf(db_err, db_err)
class TestUTF8(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.expected = ["aあ", "âéÇ", "αλΩ", "çœ¥¨≠"]
        if sys.version_info[0] < 3:
            # Python 2: convert from str to unicode.
            cls.expected = [s.decode('utf-8') for s in cls.expected]

        with pymongo.MongoClient() as c:
            c.drop_database("monary_test")
            c.monary_test.data.insert(
                {"test": my_str, "sequence": i}
                for i, my_str in enumerate(cls.expected))

    @classmethod
    def tearDownClass(cls):
        with pymongo.MongoClient() as c:
            c.drop_database("monary_test")

    def test_utf8(self):
        with monary.Monary("127.0.0.1") as m:
            data, lens, sizes = m.query(
                "monary_test", "data", {}, ["test", "test", "test"],
                ["string:12", "length", "size"], sort="sequence")
            assert (lens < sizes).all()

        for monary_bytes, monary_len, expected_str, in zip(data,
                                                           lens,
                                                           self.expected):
            monary_str = monary_bytes.decode('utf8')

            # We got the same string out from Monary as we inserted w/ PyMongo.
            assert monary_str == expected_str

            # Monary's idea of "length" == len(string).
            assert monary_len == len(expected_str)
