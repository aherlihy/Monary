# -*- coding: utf-8 -*-
# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import pymongo

import monary


expected = ["aあ".decode('utf-8'),
            "âéÇ".decode('utf-8'),
            "αλΩ".decode('utf-8'),
            "çœ¥¨≠".decode('utf-8')]


def setup():
    with pymongo.MongoClient() as c:
        c.drop_database("monary_test")
        c.monary_test.data.insert(
            {"test": my_str, "sequence": i}
            for i, my_str in enumerate(expected))


def teardown():
    with pymongo.MongoClient() as c:
        c.drop_database("monary_test")


def test_utf8():
    with monary.Monary("127.0.0.1") as m:
        data, lens, sizes = m.query(
            "monary_test", "data", {}, ["test", "test", "test"],
            ["string:12", "length", "size"], sort="sequence")
        assert (lens < sizes).all()

    for monary_bytes, monary_len, expected_str, in zip(data, lens, expected):
        monary_str = monary_bytes.decode('utf8')

        # We got the same string out from Monary as we inserted w/ PyMongo.
        assert monary_str == expected_str

        # Monary's idea of "length" == len(string).
        assert monary_len == len(expected_str)

setup()
try:
    test_utf8()
finally:
    teardown()
