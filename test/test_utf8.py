# -*- coding: utf-8 -*-
# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import sys

import pymongo

import monary


def setup():
    with pymongo.MongoClient() as c:
        c.drop_database("monary_test")
        c.monary_test.data.insert({"test": u"aあ", "sequence": 1})
        c.monary_test.data.insert({"test": u"âéÇ", "sequence": 2})
        c.monary_test.data.insert({"test": u"αλΩ", "sequence": 3})
        c.monary_test.data.insert({"test": u"çœ¥¨≠", "sequence": 4})


def teardown():
    with pymongo.MongoClient() as c:
        c.drop_database("monary_test")


def test_utf8():
    with monary.Monary("127.0.0.1") as m:
        data, lens, sizes = m.query(
            "monary_test", "data", {}, ["test", "test", "test"],
            ["string:12", "length", "size"], sort="sequence")
        assert (lens < sizes).all()

    expected = ["aあ", "âéÇ", "αλΩ", "çœ¥¨≠"]
    for x, l, y, in zip(data, lens, expected):
        if sys.version_info[0] >= 3:
            # Python 3
            x = x.decode('utf8')
        assert x == y
        assert l == len(y)
