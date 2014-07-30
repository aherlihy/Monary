# -*- coding: utf-8 -*-
# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import sys

import pymongo

import monary


def setup():
    with pymongo.Connection("127.0.0.1") as c:
        c.drop_database("monary_test")
        c.monary_test.data.insert({"test": u"aあ", "sequence": 1})
        c.monary_test.data.insert({"test": u"âéÇ", "sequence": 2})
        c.monary_test.data.insert({"test": u"αλΩ", "sequence": 3})


def teardown():
    with pymongo.Connection("127.0.0.1") as c:
        c.drop_database("monary_test")


def test_utf8():
    with monary.Monary("127.0.0.1") as m:
        data, = m.query("monary_test",
                        "data",
                        {},
                        ["test"],
                        ["string:8"],
                        sort="sequence")

    expected = ["aあ", "âéÇ", "αλΩ"]
    for x, y in zip(data, expected):
        if sys.version_info[0] >= 3:
            # Python 3
            x = x.decode('utf8')
        assert x == y
