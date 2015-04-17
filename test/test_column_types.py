# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import datetime
import random
import struct
import sys

import bson
import numpy
import pymongo

import monary
from test import db_err, unittest


PY3 = sys.version_info[0] >= 3

NUM_TEST_RECORDS = 100


@unittest.skipIf(db_err, db_err)
class TestColumnTypes(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        c = pymongo.MongoClient()
        c.drop_database("monary_test")
        db = c.monary_test
        coll = db.test_data

        random.seed(1234)  # For reproducibility.

        cls.records = []

        for i in range(NUM_TEST_RECORDS):
            if PY3:
                # Python 3.
                binary = "".join(chr(random.randint(0, 255)) for i in range(5))
                binary = binary.encode('utf-8')
            else:
                # Python 2.6 / 2.7.
                binary = "".join(chr(random.randint(0, 255)) for i in range(5))
            record = dict(
                sequence=i,
                intval=random.randint(-128, 127),
                uintval=random.randint(0, 255),
                floatval=random.uniform(-1e30, 1e30),
                boolval=(i % 2 == 0),
                dateval=(datetime.datetime(1970, 1, 1) +
                         (1 - 2 * random.randint(0, 1)) *
                         datetime.timedelta(
                             days=random.randint(0, 60 * 365),
                             seconds=random.randint(0, 24 * 60 * 60),
                             milliseconds=random.randint(0, 1000))),
                timestampval=bson.timestamp.Timestamp(
                    time=random.randint(0, 1000000),
                    inc=random.randint(0, 1000000)),
                stringval="".join(chr(ord('A') + random.randint(0, 25))
                                  for i in range(random.randint(1, 5))),
                binaryval=bson.binary.Binary(binary),
                intlistval=[random.randint(0, 100)
                            for i in range(random.randint(1, 5))],
                subdocumentval=dict(subkey=random.randint(0, 255)))
            cls.records.append(record)
        coll.insert(cls.records, safe=True)

    @classmethod
    def tearDownClass(cls):
        with pymongo.MongoClient() as c:
            c.drop_database("monary_test")

    def get_record_values(self, colname):
        return [r[colname] for r in self.records]

    def get_monary_column(self, colname, coltype):
        with monary.Monary("127.0.0.1") as m:
            [column] = m.query("monary_test",
                               "test_data",
                               {},
                               [colname],
                               [coltype],
                               sort="sequence")
        return list(column)

    def check_int_column(self, coltype):
        data = self.get_monary_column("intval", coltype)
        expected = self.get_record_values("intval")
        assert data == expected

    def check_uint_column(self, coltype):
        data = self.get_monary_column("uintval", coltype)
        expected = self.get_record_values("uintval")
        assert data == expected

    def check_float_column(self, coltype):
        data = self.get_monary_column("floatval", coltype)
        expected = self.get_record_values("floatval")
        for d, e in zip(data, expected):
            numpy.testing.assert_almost_equal(
                (d - e) / max(d, e), 0, decimal=5)

    def test_int_columns(self):
        for coltype in ["int8", "int16", "int32", "int64"]:
            yield self.check_int_column, coltype
        for coltype in ["uint8", "uint16", "uint32", "uint64"]:
            yield self.check_uint_column, coltype

    def test_float_columns(self):
        for coltype in ["float32", "float64"]:
            yield self.check_float_column, coltype

    def test_id_column(self):
        column = self.get_monary_column("_id", "id")
        data = list(map(monary.monary.mvoid_to_bson_id, column))
        expected = self.get_record_values("_id")
        assert data == expected

    def test_bool_column(self):
        data = self.get_monary_column("boolval", "bool")
        expected = self.get_record_values("boolval")
        assert data == expected

    def test_date_column(self):
        column = self.get_monary_column("dateval", "date")
        expected = self.get_record_values("dateval")
        assert [monary.mongodate_to_datetime(x) for x in column] == expected

    def test_timestamp_column(self):
        raw_data = self.get_monary_column("timestampval", "timestamp")
        data = [struct.unpack("<ii", ts) for ts in raw_data]
        timestamps = self.get_record_values("timestampval")
        expected = [(ts.time, ts.inc) for ts in timestamps]
        assert data == expected

    def test_string_column(self):
        data = self.get_monary_column("stringval", "string:5")
        expected = [s.encode('ascii')
                    for s in self.get_record_values("stringval")]
        assert data == expected

    def test_binary_column(self):
        if PY3:
            # Python 3.
            data = [bytes(x)
                    for x in self.get_monary_column("binaryval", "binary:5")]
            expected = [bytes(b)[:5]
                        for b in self.get_record_values("binaryval")]
        else:
            # Python 2.6 / 2.7.
            data = [str(x)
                    for x in self.get_monary_column("binaryval", "binary:5")]
            expected = [str(b)
                        for b in self.get_record_values("binaryval")]
        assert data == expected

    def test_nested_field(self):
        data = self.get_monary_column("subdocumentval.subkey", "int32")
        expected = [r["subkey"]
                    for r in self.get_record_values("subdocumentval")]
        assert data == expected

    def list_to_bsonable_dict(self, values):
        return monary.monary.OrderedDict((str(i), val)
                                         for i, val in enumerate(values))

    def test_bson_column(self):
        size = max(self.get_monary_column("subdocumentval", "size"))
        rawdata = self.get_monary_column("subdocumentval", "bson:%d" % size)
        expected = self.get_record_values("subdocumentval")
        data = [bson.BSON(x).decode() for x in rawdata]
        assert data == expected

    def test_type_column(self):
        # See: http://bsonspec.org/#/specification for type codes.
        data = self.get_monary_column("floatval", "type")
        expected = [1] * len(data)
        assert data == expected

        data = self.get_monary_column("stringval", "type")
        expected = [2] * len(data)
        assert data == expected

        data = self.get_monary_column("intlistval", "type")
        expected = [4] * len(data)
        assert data == expected

        data = self.get_monary_column("binaryval", "type")
        expected = [5] * len(data)
        assert data == expected

        data = self.get_monary_column("boolval", "type")
        expected = [8] * len(data)
        assert data == expected

        data = self.get_monary_column("dateval", "type")
        expected = [9] * len(data)
        assert data == expected

        data = self.get_monary_column("intval", "type")
        expected = [16] * len(data)
        assert data == expected

    def test_string_length_column(self):
        data = self.get_monary_column("stringval", "length")
        expected = [len(x) for x in self.get_record_values("stringval")]
        assert data == expected

    def test_list_length_column(self):
        data = self.get_monary_column("intlistval", "length")
        expected = [len(x) for x in self.get_record_values("intlistval")]
        assert data == expected

    def test_bson_length_column(self):
        data = self.get_monary_column("subdocumentval", "length")
        # We have only one key in the subdocument.
        expected = [1] * len(data)
        assert data == expected

    def test_string_size_column(self):
        data = self.get_monary_column("stringval", "size")
        expected = [len(x) for x in self.get_record_values("stringval")]
        assert data == expected

    def test_list_size_column(self):
        lists = self.get_record_values("intlistval")
        data = self.get_monary_column("intlistval", "size")
        expected = [len(bson.BSON.encode(self.list_to_bsonable_dict(l)))
                    for l in lists]
        assert data == expected

    def test_bson_size_column(self):
        data = self.get_monary_column("subdocumentval", "size")
        expected = [len(bson.BSON.encode(record))
                    for record in self.get_record_values("subdocumentval")]
        assert data == expected

    def test_binary_size_column(self):
        data = self.get_monary_column("binaryval", "size")
        expected = [len(x) for x in self.get_record_values("binaryval")]
        assert data == expected
