# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import datetime
import os
import random
import string
import struct
import sys
import time

import bson
import numpy as np
from numpy import ma
import pymongo

from monary import Monary, mvoid_to_bson_id, MonaryParam
from monary.monary import validate_insert_fields

PY3 = sys.version_info[0] >= 3

NUM_TEST_RECORDS = 14000

# This will hold all of the masked arrays that have inferable types. These such
# arrays are those of type bool, int, uint, and float.
TYPE_INFERABLE_ARRAYS = []
# This will be a corresponding list of types for the list above.
TYPE_INFERABLE_ARRAYS_TYPES = []

# This will hold all of the masked arrays that do not have inferable types.
# The types of timestamps and dates cannot be inferred because they are stored
# in numpy as int64's. The types of strings, binary, ObjectIds, and BSON cannot
# be inferred because they are all stored as pointers and require lengths.
NON_TYPE_INFERABLE_ARRAYS = []
# This will be a corresponding list of types for the list above.
NON_TYPE_INFERABLE_ARRAYS_TYPES = []

# These values are being cached to speed up the tests.
bool_arr = None
int8_arr = None
int16_arr = None
int32_arr = None
int64_arr = None
uint8_arr = None
uint16_arr = None
uint32_arr = None
uint64_arr = None
float32_arr = None
float64_arr = None
timestamp_arr = None
date_arr = None
string_arr = None
bin_arr = None

# This is used for sorting results during queries to ensure the returned values
# are in the same order as the values inserted.
seq = ma.masked_array(np.arange(NUM_TEST_RECORDS, dtype="int64"),
                      np.zeros(NUM_TEST_RECORDS))
seq_type = "int64"


def NTR():
    return range(NUM_TEST_RECORDS)


def rand_bools():
    return [bool(random.getrandbits(1)) for _ in NTR()]


def make_ma(data, dtype):
    return ma.masked_array(data, rand_bools(), dtype=dtype)


def random_timestamp():
    ts = bson.timestamp.Timestamp(
        time=random.randint(0, 2147483647),
        inc=random.randint(0, 2147483647))
    sp = struct.pack("<ii", ts.time, ts.inc)
    return struct.unpack('@q', sp)[0]


def random_date():
    t = datetime.datetime(1970, 1, 1) + \
        (1 - 2 * random.randint(0, 1)) * \
        datetime.timedelta(days=random.randint(0, 60 * 365),
                           seconds=random.randint(0, 24 * 3600),
                           milliseconds=random.randint(0, 1000))
    return time.mktime(t.timetuple())


def random_string(str_len):
    return "".join(random.choice(string.ascii_letters)
                   for _ in range(str_len))


def setup():
    global TYPE_INFERABLE_ARRAYS, TYPE_INFERABLE_ARRAYS_TYPES
    global NON_TYPE_INFERABLE_ARRAYS, NON_TYPE_INFERABLE_ARRAYS_TYPES
    global bool_arr, int8_arr, int16_arr, int32_arr, int64_arr, uint8_arr
    global uint16_arr, uint32_arr, uint64_arr, float32_arr, float64_arr
    global timestamp_arr, date_arr, string_arr, bin_arr, seq
    with pymongo.MongoClient() as c:
        c.drop_database("monary_test")
    random.seed(1234)  # For reproducibility.

    bool_arr = make_ma(rand_bools(), "bool")
    TYPE_INFERABLE_ARRAYS.append(bool_arr)
    TYPE_INFERABLE_ARRAYS_TYPES.append("bool")

    int8_arr = make_ma([random.randint(0 - 2 ** 4, 2 ** 4 - 1)
                        for _ in NTR()], "int8")
    TYPE_INFERABLE_ARRAYS.append(int8_arr)
    TYPE_INFERABLE_ARRAYS_TYPES.append("int8")

    int16_arr = make_ma([random.randint(0 - 2 ** 8, 2 ** 8 - 1)
                         for _ in NTR()], "int16")
    TYPE_INFERABLE_ARRAYS.append(int16_arr)
    TYPE_INFERABLE_ARRAYS_TYPES.append("int16")

    int32_arr = make_ma([random.randint(0 - 2 ** 16, 2 ** 16 - 1)
                         for _ in NTR()], "int32")
    TYPE_INFERABLE_ARRAYS.append(int32_arr)
    TYPE_INFERABLE_ARRAYS_TYPES.append("int32")

    int64_arr = make_ma([random.randint(0 - 2 ** 32, 2 ** 32 - 1)
                         for _ in NTR()], "int64")
    TYPE_INFERABLE_ARRAYS.append(int64_arr)
    TYPE_INFERABLE_ARRAYS_TYPES.append("int64")

    uint8_arr = make_ma([random.randint(0, 2 ** 8 - 1)
                         for _ in NTR()], "uint8")
    TYPE_INFERABLE_ARRAYS.append(uint8_arr)
    TYPE_INFERABLE_ARRAYS_TYPES.append("uint8")

    uint16_arr = make_ma([random.randint(0, 2 ** 16 - 1)
                          for _ in NTR()], "uint16")
    TYPE_INFERABLE_ARRAYS.append(uint16_arr)
    TYPE_INFERABLE_ARRAYS_TYPES.append("uint16")

    uint32_arr = make_ma([random.randint(0, 2 ** 32 - 1)
                          for _ in NTR()], "uint32")
    TYPE_INFERABLE_ARRAYS.append(uint32_arr)
    TYPE_INFERABLE_ARRAYS_TYPES.append("uint32")

    uint64_arr = make_ma([random.randint(0, 2 ** 64 - 1)
                          for _ in NTR()], "uint64")
    TYPE_INFERABLE_ARRAYS.append(uint64_arr)
    TYPE_INFERABLE_ARRAYS_TYPES.append("uint64")

    float32_arr = make_ma([random.uniform(-1e30, 1e30)
                           for _ in NTR()], "float32")
    TYPE_INFERABLE_ARRAYS.append(float32_arr)
    TYPE_INFERABLE_ARRAYS_TYPES.append("float32")

    float64_arr = make_ma([random.uniform(-1e30, 1e30)
                           for _ in NTR()], "float64")
    TYPE_INFERABLE_ARRAYS.append(float64_arr)
    TYPE_INFERABLE_ARRAYS_TYPES.append("float64")

    timestamp_arr = make_ma([random_timestamp() for _ in NTR()], "uint64")
    NON_TYPE_INFERABLE_ARRAYS.append(timestamp_arr)
    NON_TYPE_INFERABLE_ARRAYS_TYPES.append("timestamp")

    date_arr = make_ma([random_date() for _ in NTR()], "int64")
    NON_TYPE_INFERABLE_ARRAYS.append(date_arr)
    NON_TYPE_INFERABLE_ARRAYS_TYPES.append("date")

    string_arr = make_ma([random_string(10) for _ in NTR()], "S10")
    NON_TYPE_INFERABLE_ARRAYS.append(string_arr)
    NON_TYPE_INFERABLE_ARRAYS_TYPES.append("string:10")

    bin_arr = make_ma([os.urandom(20) for _ in NTR()], "<V20")
    NON_TYPE_INFERABLE_ARRAYS.append(bin_arr)
    NON_TYPE_INFERABLE_ARRAYS_TYPES.append("binary:20")


def test_insert_and_retrieve_no_types():
    params = MonaryParam.from_lists(
        TYPE_INFERABLE_ARRAYS + [seq],
        ["x1", "x2", "x3", "x4", "x5", "x6", "x7", "x8", "x9",
         "x10", "x11", "sequence"])
    with Monary() as m:
        ids = m.insert("monary_test", "data", params)
        assert len(ids) == ids.count() == NUM_TEST_RECORDS
        retrieved = m.query("monary_test", "data", {},
                            ["x1", "x2", "x3", "x4", "x5", "x6", "x7", "x8",
                             "x9", "x10", "x11"],
                            TYPE_INFERABLE_ARRAYS_TYPES, sort="sequence")
        for data, expected in zip(retrieved, TYPE_INFERABLE_ARRAYS):
            assert data.count() == expected.count()
            assert (data == expected).all()
    teardown()


def test_insert_and_retrieve():
    arrays = TYPE_INFERABLE_ARRAYS + NON_TYPE_INFERABLE_ARRAYS + [seq]
    types = TYPE_INFERABLE_ARRAYS_TYPES + NON_TYPE_INFERABLE_ARRAYS_TYPES \
        + [seq_type]
    params = MonaryParam.from_lists(
        arrays, ["x1", "x2", "x3", "x4", "x5", "x6", "x7", "x8", "x9", "x10",
                 "x11", "x12", "x13", "x14", "x15", "sequence"], types)
    with Monary() as m:
        ids = m.insert("monary_test", "data", params)
        assert len(ids) == ids.count() == NUM_TEST_RECORDS
        retrieved = m.query("monary_test", "data", {},
                            ["x1", "x2", "x3", "x4", "x5", "x6", "x7", "x8",
                             "x9", "x10", "x11", "x12", "x13", "x14", "x15",
                             "sequence"], types, sort="sequence")
        for data, expected in zip(retrieved, arrays):
            assert data.count() == expected.count()
            if("V" in str(data.dtype)):
                # Need to convert binary data.
                fun = str
                if PY3:
                    fun = bytes
                data = [fun(data[i])
                        for i in range(len(data))
                        if not data.mask[i]]
                expected = [fun(expected[i])
                            for i in range(len(expected))
                            if not expected.mask[i]]
                # Make these into np.arrays so .all() still works.
                data = np.array([data == expected])
                expected = np.array([True])
            assert (data == expected).all()
    teardown()


def test_oid():
    with Monary() as m:
        # Insert documents to generate ObjectIds.
        ids = m.insert("monary_test", "data",
                       MonaryParam.from_lists([bool_arr, seq],
                                              ["dummy", "sequence"]))
        assert len(ids) == ids.count() == NUM_TEST_RECORDS
        # Increment the sequence so sorting still works
        seq2 = seq + NUM_TEST_RECORDS

        ids2 = m.insert(
            "monary_test", "data",
            MonaryParam.from_lists(
                [ids, seq2], ["oid", "sequence"], ["id", "int64"]))
        assert len(ids2) == ids.count() == NUM_TEST_RECORDS
        # Get back the ids from the original insert (_id) and the ids that
        # were manually inserted (oid)
        retrieved = m.query("monary_test", "data", {},
                            ["_id", "oid"], ["id", "id"], sort="sequence")
        # These should be equal to ``ids`` from the top of this test.
        expected = retrieved[0][:NUM_TEST_RECORDS]
        # This is what we get back when querying for the ObjectIds
        # that were inserted manually above.
        data = retrieved[1][NUM_TEST_RECORDS:]
        assert len(expected) == len(data)
        assert len(expected) == expected.count()
        assert len(data) == data.count()
        for d, e in zip(data, expected):
            assert mvoid_to_bson_id(d) == mvoid_to_bson_id(e)
    teardown()


def test_insert_field_validation():
    good = [
        ["a", "b", "c"],
        ["a", "b.a", "b.b", "c"],
        ["a.a", "a.b", "a.c", "b", "c.a", "c.b", "c.c"],
        ["a.b.c.d.e.f", "g.h.i.j.k", "l.m.n.o.p.q.r.s.t.u", "b.c.d"],
    ]
    bad = [
        ["a", "b", "a"],  # "a" occurs twice.
        ["a", "b.c", "b.c.d"],  # "b.c" is duplicated.
        ["a.a", "a.b", "b", "c.a", "c.b", "a.b.c"],  # "a.b" is duplicated.
        ["a.a.a", "a.a.b", "a.c.b", "b", "c.a", "c."]  # "c." ends in ".".
    ]
    for g in good:
        try:
            validate_insert_fields(g)
        except ValueError:
            assert False, "%r should have been valid" % g
    for b in bad:
        try:
            validate_insert_fields(b)
            assert False, "%r should not have been valid" % b
        except ValueError:
            pass


def test_nested_insert():
    squares = np.arange(NUM_TEST_RECORDS) ** 2
    squares = ma.masked_array(squares, np.zeros(NUM_TEST_RECORDS),
                              dtype="float64")
    random = np.random.uniform(0, 5, NUM_TEST_RECORDS)
    random = ma.masked_array(random, np.zeros(NUM_TEST_RECORDS),
                             dtype="float64")
    unmasked = ma.masked_array(rand_bools(), np.zeros(NUM_TEST_RECORDS),
                               dtype="bool")
    masked = ma.masked_array(rand_bools(), np.ones(NUM_TEST_RECORDS),
                             dtype="bool")
    with Monary() as m:
        m.insert(
            "monary_test", "data",
            MonaryParam.from_lists(
                [squares, random, seq, unmasked, masked],
                ["data.sqr", "data.rand", "sequence", "x.y.real", "x.y.fake"]))
    with pymongo.MongoClient() as c:
        col = c.monary_test.data
        for i, doc in enumerate(col.find().sort(
                [("sequence", pymongo.ASCENDING)])):
            assert doc["sequence"] == i
            assert random[i] == doc["data"]["rand"]
            assert squares[i] == doc["data"]["sqr"]
            assert "fake" not in doc["x"]["y"]
            assert unmasked[i] == doc["x"]["y"]["real"]
    teardown()


def test_retrieve_nested():
    arrays = [bool_arr, int8_arr, int16_arr, int32_arr, int64_arr, float32_arr,
              float64_arr, string_arr, seq]
    with Monary() as m:
        m.insert(
            "monary_test", "data",
            MonaryParam.from_lists(
                arrays,
                ["a.b.c.d.e.f.g.h.x1", "a.b.c.d.e.f.g.h.x2",
                 "a.b.c.d.e.f.g.h.x3", "a.b.c.d.e.f.g.h.x4",
                 "a.b.c.d.e.f.g.h.x5", "a.b.c.d.e.f.g.h.x6",
                 "a.b.c.d.e.f.g.h.x7", "a.b.c.d.e.f.g.h.x8",
                 "sequence"],
                ["bool", "int8", "int16", "int32", "int64",
                 "float32", "float64", "string:10", "int64"]))
    with pymongo.MongoClient() as c:
        col = c.monary_test.data
        for i, doc in enumerate(col.find().sort(
                [("sequence", pymongo.ASCENDING)])):
            assert doc["sequence"] == i
            for j in range(8):
                if not arrays[j].mask[i]:
                    data = arrays[j][i]
                    exp = doc["a"]["b"]["c"]["d"]["e"]["f"]["g"]["h"]
                    exp = exp["x" + str(j + 1)]
                    if PY3 and isinstance(data, bytes):
                        data = data.decode("ascii")
    teardown()


def test_insert_bson():
    docs = []
    for i in NTR():
        doc = {"subdoc": {"num": random.randint(0, 255)}}
        if i % 2 == 0:
            doc["subdoc"]["bool"] = bool(random.getrandbits(1))
        if i % 3 == 0:
            doc["float"] = random.uniform(-1e30, 1e30)
        docs.append(doc)
    encoded = [bson.BSON.encode(d) for d in docs]
    max_len = max(map(len, encoded))
    encoded = ma.masked_array(encoded, np.zeros(NUM_TEST_RECORDS),
                              "<V%d" % max_len)
    with Monary() as m:
        m.insert(
            "monary_test", "data",
            MonaryParam.from_lists([encoded, seq],
                                   ["doc", "sequence"],
                                   ["bson:%d" % max_len, "int64"]))
    with pymongo.MongoClient() as c:
        col = c.monary_test.data
        for i, doc in enumerate(col.find().sort(
                [("sequence", pymongo.ASCENDING)])):
            assert doc["sequence"] == i
            assert doc["doc"] == docs[i]
    teardown()


def test_custom_id():
    f_unmasked = ma.masked_array(np.arange(NUM_TEST_RECORDS, dtype="float64"),
                                 np.zeros(NUM_TEST_RECORDS))
    # To avoid collision with seq.
    f_unmasked += 0.5
    with Monary() as m:
        id_seq = m.insert(
            "monary_test", "data",
            MonaryParam.from_lists([int16_arr, seq], ["num", "_id"]))
        assert len(id_seq) == id_seq.count() == NUM_TEST_RECORDS
        assert (id_seq == seq.data).all()
        id_float = m.insert(
            "monary_test", "data",
            MonaryParam.from_lists([seq, date_arr, f_unmasked],
                                   ["sequence", "x.date", "_id"],
                                   ["int64", "date", "float64"]))
        assert len(id_float) == id_float.count() == NUM_TEST_RECORDS
        assert (id_float == f_unmasked.data).all()
        # BSON type 18 is int64.
        data, = m.query("monary_test", "data", {"_id": {"$type": 18}},
                        ["_id"], ["int64"], sort="_id")
        assert len(data) == data.count() == NUM_TEST_RECORDS
        assert (data == seq).all()
        # BSON type 1 is double (float64).
        data, = m.query("monary_test", "data", {"_id": {"$type": 1}},
                        ["_id"], ["float64"], sort="sequence")
        assert len(data) == data.count() == NUM_TEST_RECORDS
        assert (data == f_unmasked).all()
    teardown()


def test_insert_errors():
    with Monary() as m:
        a = ma.masked_array([1, 3], [False] * 2, dtype="int8")
        b = ma.masked_array([1, 2, 3, 4], [False] * 4, dtype="int8")
        a_id = m.insert("monary_test", "data", [MonaryParam(a, "_id")])
        assert len(a_id) == a_id.count() == len(a)
        b_id = m.insert("monary_test", "data", [MonaryParam(b, "_id")])
        assert len(b_id) == len(b)
        assert b_id.count() == len(b) - len(a)
        teardown()

        # ``threes`` is a list of numbers counting up by 3, i.e. 0 3 6 9 ...
        num_threes = int(NUM_TEST_RECORDS / 3) + 1
        threes = np.arange(num_threes, dtype="int64")
        threes *= 3
        threes = ma.masked_array(threes, np.zeros(num_threes))
        m.insert("monary_test", "data", [MonaryParam(threes, "_id")])

        nums = ma.masked_array(
            np.arange(NUM_TEST_RECORDS, dtype="int64"),
            np.zeros(NUM_TEST_RECORDS))
        ids = m.insert("monary_test", "data", [MonaryParam(nums, "_id")])

        assert len(ids) == len(nums)
        assert ids.count() == len(nums) - len(threes)
        # Everything that's a 'three' should be masked.
        assert ids.mask[::3].all()
        # Nothing that's not a 'three' should be masked.
        assert not ids.mask[1::3].any()
        assert not ids.mask[2::3].any()
    teardown()


def teardown():
    with pymongo.MongoClient() as c:
        c.drop_database("monary_test")
