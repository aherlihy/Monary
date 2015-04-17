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
import pymongo

import monary
from test import db_err, unittest

PY3 = sys.version_info[0] >= 3

NUM_TEST_RECORDS = 14000


@unittest.skipIf(db_err, db_err)
class TestInserts(unittest.TestCase):

    # This will hold all of the masked arrays that have inferable types.
    # These such arrays are those of type bool, int, uint, and float.
    TYPE_INFERABLE_ARRAYS = []
    # This will be a corresponding list of types for the list above.
    TYPE_INFERABLE_ARRAYS_TYPES = []

    # This will hold all of the masked arrays that do not have inferable types.
    # The types of timestamps and dates cannot be inferred because they are
    # stored in numpy as int64's. The types of strings, binary, ObjectIds, and
    # BSON cannot be inferred because they are all stored as pointers and
    # require lengths.
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

    # This is used for sorting results during queries to ensure the returned
    # values are in the same order as the values inserted.
    seq = np.ma.masked_array(np.arange(NUM_TEST_RECORDS, dtype=np.int64),
                             np.zeros(NUM_TEST_RECORDS))
    seq_type = "int64"

    @classmethod
    def setUpClass(cls):
        with pymongo.MongoClient() as c:
            c.drop_database("monary_test")

        random.seed(1234)  # For reproducibility.

        def ntr(): return range(NUM_TEST_RECORDS)

        def rand_bools(): return [bool(random.getrandbits(1)) for _ in ntr()]

        def make_ma(data, dtype): return np.ma.masked_array(data,
                                                            rand_bools(),
                                                            dtype=dtype)

        cls.bool_arr = make_ma(rand_bools(), "bool")
        cls.TYPE_INFERABLE_ARRAYS.append(cls.bool_arr)
        cls.TYPE_INFERABLE_ARRAYS_TYPES.append("bool")

        cls.int8_arr = make_ma([random.randint(0 - 2 ** 4, 2 ** 4 - 1)
                                for _ in ntr()], "int8")
        cls.TYPE_INFERABLE_ARRAYS.append(cls.int8_arr)
        cls.TYPE_INFERABLE_ARRAYS_TYPES.append("int8")

        cls.int16_arr = make_ma([random.randint(0 - 2 ** 8, 2 ** 8 - 1)
                                 for _ in ntr()], "int16")
        cls.TYPE_INFERABLE_ARRAYS.append(cls.int16_arr)
        cls.TYPE_INFERABLE_ARRAYS_TYPES.append("int16")

        cls.int32_arr = make_ma([random.randint(0 - 2 ** 16, 2 ** 16 - 1)
                                 for _ in ntr()], "int32")
        cls.TYPE_INFERABLE_ARRAYS.append(cls.int32_arr)
        cls.TYPE_INFERABLE_ARRAYS_TYPES.append("int32")

        cls.int64_arr = make_ma([random.randint(0 - 2 ** 32, 2 ** 32 - 1)
                                 for _ in ntr()], "int64")
        cls.TYPE_INFERABLE_ARRAYS.append(cls.int64_arr)
        cls.TYPE_INFERABLE_ARRAYS_TYPES.append("int64")

        cls.uint8_arr = make_ma([random.randint(0, 2 ** 8 - 1)
                                 for _ in ntr()], "uint8")
        cls.TYPE_INFERABLE_ARRAYS.append(cls.uint8_arr)
        cls.TYPE_INFERABLE_ARRAYS_TYPES.append("uint8")

        cls.uint16_arr = make_ma([random.randint(0, 2 ** 16 - 1)
                                  for _ in ntr()], "uint16")
        cls.TYPE_INFERABLE_ARRAYS.append(cls.uint16_arr)
        cls.TYPE_INFERABLE_ARRAYS_TYPES.append("uint16")

        cls.uint32_arr = make_ma([random.randint(0, 2 ** 32 - 1)
                                  for _ in ntr()], "uint32")
        cls.TYPE_INFERABLE_ARRAYS.append(cls.uint32_arr)
        cls.TYPE_INFERABLE_ARRAYS_TYPES.append("uint32")

        cls.uint64_arr = make_ma([random.randint(0, 2 ** 64 - 1)
                                  for _ in ntr()], "uint64")
        cls.TYPE_INFERABLE_ARRAYS.append(cls.uint64_arr)
        cls.TYPE_INFERABLE_ARRAYS_TYPES.append("uint64")

        cls.float32_arr = make_ma([random.uniform(-1e30, 1e30)
                                   for _ in ntr()], "float32")
        cls.TYPE_INFERABLE_ARRAYS.append(cls.float32_arr)
        cls.TYPE_INFERABLE_ARRAYS_TYPES.append("float32")

        cls.float64_arr = make_ma([random.uniform(-1e30, 1e30)
                                   for _ in ntr()], "float64")
        cls.TYPE_INFERABLE_ARRAYS.append(cls.float64_arr)
        cls.TYPE_INFERABLE_ARRAYS_TYPES.append("float64")

        # Calculate random timestamp.
        ts = bson.timestamp.Timestamp(
            time=random.randint(0, 2147483647),
            inc=random.randint(0, 2147483647))
        sp = struct.pack("<ii", ts.time, ts.inc)
        random_timestamp = struct.unpack('@q', sp)[0]

        cls.timestamp_arr = make_ma([random_timestamp for _ in ntr()],
                                    "uint64")
        cls.NON_TYPE_INFERABLE_ARRAYS.append(cls.timestamp_arr)
        cls.NON_TYPE_INFERABLE_ARRAYS_TYPES.append("timestamp")

        # Calculate random date.
        t = (datetime.datetime(1970, 1, 1) +
             (1 - 2 * random.randint(0, 1)) *
             datetime.timedelta(days=random.randint(0, 60 * 365),
                                seconds=random.randint(0, 24 * 3600),
                                milliseconds=random.randint(0, 1000)))
        random_date = time.mktime(t.timetuple())

        cls.date_arr = make_ma([random_date for _ in ntr()], "int64")
        cls.NON_TYPE_INFERABLE_ARRAYS.append(cls.date_arr)
        cls.NON_TYPE_INFERABLE_ARRAYS_TYPES.append("date")

        # Calculate random string.
        random_string = "".join(random.choice(string.ascii_letters)
                                for _ in range(10))

        cls.string_arr = make_ma([random_string for _ in ntr()], "S10")
        cls.NON_TYPE_INFERABLE_ARRAYS.append(cls.string_arr)
        cls.NON_TYPE_INFERABLE_ARRAYS_TYPES.append("string:10")

        cls.bin_arr = make_ma([os.urandom(20) for _ in ntr()], "<V20")
        cls.NON_TYPE_INFERABLE_ARRAYS.append(cls.bin_arr)
        cls.NON_TYPE_INFERABLE_ARRAYS_TYPES.append("binary:20")

    @classmethod
    def tearDownClass(cls):
        with pymongo.MongoClient() as c:
            c.drop_database("monary_test")

    def test_insert_and_retrieve_no_types(self):
        params = monary.MonaryParam.from_lists(
            self.TYPE_INFERABLE_ARRAYS + [self.seq],
            ["x1", "x2", "x3", "x4", "x5", "x6", "x7", "x8", "x9",
             "x10", "x11", "sequence"])
        with monary.Monary() as m:
            ids = m.insert("monary_test", "data", params)
            assert len(ids) == ids.count() == NUM_TEST_RECORDS
            retrieved = m.query("monary_test",
                                "data",
                                {},
                                ["x1", "x2", "x3", "x4", "x5",
                                 "x6", "x7", "x8", "x9", "x10", "x11"],
                                self.TYPE_INFERABLE_ARRAYS_TYPES,
                                sort="sequence")
            for data, expected in zip(retrieved, self.TYPE_INFERABLE_ARRAYS):
                assert data.count() == expected.count()
                assert (data == expected).all()
        with pymongo.MongoClient() as c:
            c.drop_database("monary_test")

    def test_insert_and_retrieve(self):
        arrays = (self.TYPE_INFERABLE_ARRAYS +
                  self.NON_TYPE_INFERABLE_ARRAYS +
                  [self.seq])
        types = (self.TYPE_INFERABLE_ARRAYS_TYPES +
                 self.NON_TYPE_INFERABLE_ARRAYS_TYPES +
                 [self.seq_type])
        params = monary.MonaryParam.from_lists(
            arrays, ["x1", "x2", "x3", "x4", "x5",
                     "x6", "x7", "x8", "x9", "x10",
                     "x11", "x12", "x13", "x14", "x15", "sequence"], types)
        with monary.Monary() as m:
            ids = m.insert("monary_test", "data", params)
            assert len(ids) == ids.count() == NUM_TEST_RECORDS
            retrieved = m.query("monary_test", "data", {},
                                ["x1", "x2", "x3", "x4", "x5",
                                 "x6", "x7", "x8", "x9", "x10",
                                 "x11", "x12", "x13", "x14", "x15",
                                 "sequence"], types, sort="sequence")
            for data, expected in zip(retrieved, arrays):
                assert data.count() == expected.count()
                if "V" in str(data.dtype):
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
        with pymongo.MongoClient() as c:
            c.drop_database("monary_test")

    def test_oid(self):
        with monary.Monary() as m:
            # Insert documents to generate ObjectIds.
            ids = m.insert("monary_test", "data",
                           monary.MonaryParam.from_lists(
                               [self.bool_arr, self.seq],
                               ["dummy", "sequence"]))
            assert len(ids) == ids.count() == NUM_TEST_RECORDS
            # Increment the sequence so sorting still works
            seq2 = self.seq + NUM_TEST_RECORDS

            ids2 = m.insert(
                "monary_test", "data",
                monary.MonaryParam.from_lists(
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
                assert monary.mvoid_to_bson_id(d) == monary.mvoid_to_bson_id(e)
        with pymongo.MongoClient() as c:
            c.drop_database("monary_test")

    def test_insert_field_validation(self):
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
                monary.monary.validate_insert_fields(g)
            except ValueError:
                assert False, "%r should have been valid" % g
        for b in bad:
            try:
                monary.monary.validate_insert_fields(b)
                assert False, "%r should not have been valid" % b
            except ValueError:
                pass

    def test_nested_insert(self):
        squares = np.arange(NUM_TEST_RECORDS) ** 2
        squares = np.ma.masked_array(squares, np.zeros(NUM_TEST_RECORDS),
                                     dtype="float64")
        rand = np.random.uniform(0, 5, NUM_TEST_RECORDS)
        rand = np.ma.masked_array(rand, np.zeros(NUM_TEST_RECORDS),
                                  dtype="float64")
        rand_bools = [bool(random.getrandbits(1))
                      for _ in range(NUM_TEST_RECORDS)]
        unmasked = np.ma.masked_array(rand_bools, np.zeros(NUM_TEST_RECORDS),
                                      dtype="bool")
        rand_bools = [bool(random.getrandbits(1))
                      for _ in range(NUM_TEST_RECORDS)]
        masked = np.ma.masked_array(rand_bools, np.ones(NUM_TEST_RECORDS),
                                    dtype="bool")
        with monary.Monary() as m:
            m.insert(
                "monary_test", "data",
                monary.MonaryParam.from_lists(
                    [squares, rand, self.seq, unmasked, masked],
                    ["data.sqr", "data.rand", "sequence",
                     "x.y.real", "x.y.fake"]))
        with pymongo.MongoClient() as c:
            col = c.monary_test.data
            for i, doc in enumerate(col.find().sort(
                    [("sequence", pymongo.ASCENDING)])):
                assert doc["sequence"] == i
                assert rand[i] == doc["data"]["rand"]
                assert squares[i] == doc["data"]["sqr"]
                assert "fake" not in doc["x"]["y"]
                assert unmasked[i] == doc["x"]["y"]["real"]
        with pymongo.MongoClient() as c:
            c.drop_database("monary_test")

    def test_retrieve_nested(self):
        arrays = [self.bool_arr, self.int8_arr, self.int16_arr, self.int32_arr,
                  self.int64_arr, self.float32_arr, self.float64_arr,
                  self.string_arr, self.seq]
        with monary.Monary() as m:
            m.insert(
                "monary_test", "data",
                monary.MonaryParam.from_lists(
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
        with pymongo.MongoClient() as c:
            c.drop_database("monary_test")

    def test_insert_bson(self):
        docs = []
        for i in range(NUM_TEST_RECORDS):
            doc = {"subdoc": {"num": random.randint(0, 255)}}
            if i % 2 == 0:
                doc["subdoc"]["bool"] = bool(random.getrandbits(1))
            if i % 3 == 0:
                doc["float"] = random.uniform(-1e30, 1e30)
            docs.append(doc)
        encoded = [bson.BSON.encode(d) for d in docs]
        max_len = max(map(len, encoded))
        encoded = np.ma.masked_array(encoded, np.zeros(NUM_TEST_RECORDS),
                                     "<V%d" % max_len)
        with monary.Monary() as m:
            m.insert(
                "monary_test", "data",
                monary.MonaryParam.from_lists([encoded, self.seq],
                                              ["doc", "sequence"],
                                              ["bson:%d" % max_len, "int64"]))
        with pymongo.MongoClient() as c:
            col = c.monary_test.data
            for i, doc in enumerate(col.find().sort(
                    [("sequence", pymongo.ASCENDING)])):
                assert doc["sequence"] == i
                assert doc["doc"] == docs[i]
        with pymongo.MongoClient() as c:
            c.drop_database("monary_test")

    def test_custom_id(self):
        f_unmasked = np.ma.masked_array(
            np.arange(
                NUM_TEST_RECORDS,
                dtype=np.float64),
            np.zeros(NUM_TEST_RECORDS))
        # To avoid collision with seq.
        f_unmasked += 0.5
        with monary.Monary() as m:
            id_seq = m.insert(
                "monary_test",
                "data",
                monary.MonaryParam.from_lists(
                    [self.int16_arr, self.seq],
                    ["num", "_id"]))
            assert len(id_seq) == id_seq.count() == NUM_TEST_RECORDS
            assert (id_seq == self.seq.data).all()
            id_float = m.insert(
                "monary_test", "data",
                monary.MonaryParam.from_lists(
                    [self.seq, self.date_arr, f_unmasked],
                    ["sequence", "x.date", "_id"],
                    ["int64", "date", "float64"]))
            assert len(id_float) == id_float.count() == NUM_TEST_RECORDS
            assert (id_float == f_unmasked.data).all()
            # BSON type 18 is int64.
            data, = m.query("monary_test", "data", {"_id": {"$type": 18}},
                            ["_id"], ["int64"], sort="_id")
            assert len(data) == data.count() == NUM_TEST_RECORDS
            assert (data == self.seq).all()
            # BSON type 1 is double (float64).
            data, = m.query("monary_test", "data", {"_id": {"$type": 1}},
                            ["_id"], ["float64"], sort="sequence")
            assert len(data) == data.count() == NUM_TEST_RECORDS
            assert (data == f_unmasked).all()
        with pymongo.MongoClient() as c:
            c.drop_database("monary_test")

    def test_insert_errors(self):
        with monary.Monary() as m:
            a = np.ma.masked_array([1, 3], [False] * 2, dtype="int8")
            b = np.ma.masked_array([1, 2, 3, 4], [False] * 4, dtype="int8")
            a_id = m.insert("monary_test",
                            "data",
                            [monary.MonaryParam(a, "_id")])
            assert len(a_id) == a_id.count() == len(a)
            b_id = m.insert("monary_test",
                            "data",
                            [monary.MonaryParam(b, "_id")])
            assert len(b_id) == len(b)
            assert b_id.count() == len(b) - len(a)
            with pymongo.MongoClient() as c:
                c.drop_database("monary_test")

            # ``threes`` is a list of numbers counting up by 3, i.e. 0 3 6 ...
            num_threes = int(NUM_TEST_RECORDS / 3) + 1
            threes = np.arange(num_threes, dtype=np.int64)
            threes *= 3
            threes = np.ma.masked_array(threes, np.zeros(num_threes))
            m.insert("monary_test",
                     "data",
                     [monary.MonaryParam(threes, "_id")])

            nums = np.ma.masked_array(
                np.arange(NUM_TEST_RECORDS, dtype=np.int64),
                np.zeros(NUM_TEST_RECORDS))
            ids = m.insert("monary_test", "data",
                           [monary.MonaryParam(nums, "_id")])

            assert len(ids) == len(nums)
            assert ids.count() == len(nums) - len(threes)
            # Everything that's a 'three' should be masked.
            assert ids.mask[::3].all()
            # Nothing that's not a 'three' should be masked.
            assert not ids.mask[1::3].any()
            assert not ids.mask[2::3].any()
        with pymongo.MongoClient() as c:
            c.drop_database("monary_test")
