# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import datetime
import random
import struct
import sys

import bson
import numpy as np
import pymongo

import monary
from test import db_err, unittest


PY3 = sys.version_info[0] >= 3

NUM_TEST_RECORDS = 100


@unittest.skipIf(db_err, db_err)
class TestColumnTypes(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with pymongo.MongoClient() as c:
            c.drop_database("monary_test")

        random.seed(1234)  # For reproducibility.

        cls.records = []

        def ma(typ):
            return np.ma.masked_array(np.empty(NUM_TEST_RECORDS),
                                      np.zeros(NUM_TEST_RECORDS),
                                      typ)

        seq_ma = ma("int32")
        int_ma = ma("int32")
        uint_ma = ma("uint32")
        float_ma = ma("float64")
        bool_ma = ma("bool")
        date_ma = ma("int64")
        timestamp_ma = ma("uint64")
        string_ma = ma("S6")
        binary_ma = np.ma.masked_array(["".encode('utf-8')]*NUM_TEST_RECORDS,
                                       np.zeros(NUM_TEST_RECORDS),
                                       "<V6")
        #list_ma = #TODO: FIX
        doc_ma = np.ma.masked_array(["".encode('utf-8')]*NUM_TEST_RECORDS,
                                       np.zeros(NUM_TEST_RECORDS),
                                       "<V100")



        for i in range(NUM_TEST_RECORDS):
            if PY3:
                # Python 3.
                binary = "".join(chr(random.randint(0, 255)) for j in range(5))
                binary = binary.encode('utf-8')
            else:
                # Python 2.6 / 2.7.
                binary = "".join(chr(random.randint(0, 255)) for j in range(5))
            seq_ma[i] = i
            int_ma[i] = random.randint(-128, 127)
            uint_ma[i] = random.randint(0, 255)
            float_ma[i] = random.uniform(-1e30, 1e30)
            bool_ma[i] = (i % 2 == 0)
            date_ma[i] = (datetime.datetime(1970, 1, 1) -
                          datetime.datetime.now()).total_seconds()
            timestamp_ma[i] = bson.timestamp.Timestamp(
                time=random.randint(0, 1000000),
                inc=random.randint(0, 1000000)).time
            s ="".join(chr(ord('A') + random.randint(0, 25))
                              for i in range(random.randint(1, 5)))
            print " S =", s, len(s)
            string_ma[i] = s
            print "[i]=", string_ma[i], len(string_ma[i])
            binary_ma[i] = bson.binary.Binary(binary)
            #intlist_ma[i] =[random.randint(0, 100)
            #            for i in range(random.randint(1, 5))],
            doc = dict(subkey=random.randint(0, 255))
            doc_ma[i] = bson.BSON.encode(doc)
            cls.records.append(dict(sequence=seq_ma[i], intval=int_ma[i],
                                    uintval=uint_ma[i], floatval=float_ma[i],
                                    boolval=bool_ma[i], dateval=date_ma[i],
                                    timestampval=timestamp_ma[i],
                                    stringval=string_ma[i],
                                    binaryval=binary_ma[i],
                                    #intlistval=,
                                    subdocumentval=doc))

        param = monary.MonaryParam.from_lists(
            [seq_ma, int_ma, uint_ma, float_ma, bool_ma, date_ma,
             timestamp_ma, string_ma, binary_ma, doc_ma],
            ["sequence", "intval", "uintval", "floatval", "boolval", "dateval",
             "timestampval", "stringval", "binaryval", "subdocumentval"],
            ["int32", "int32", "uint32", "float64", "bool", "int64",
             "uint64", "string:6", "binary:6", "bson:100"])
        with monary.Monary() as m:
            m.insert("monary_test", "test_data", param)

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
            np.testing.assert_almost_equal(
                (d - e) / max(d, e), 0, decimal=5)

    def test_int_columns(self):
        for coltype in ["int8", "int16", "int32", "int64"]:
            yield self.check_int_column, coltype
        for coltype in ["uint8", "uint16", "uint32", "uint64"]:
            yield self.check_uint_column, coltype

    def test_float_columns(self):
        for coltype in ["float32", "float64"]:
            yield self.check_float_column, coltype

    #def test_id_column(self):
    #    column = self.get_monary_column("_id", "id")
    #    data = list(map(monary.monary.mvoid_to_bson_id, column))
    #    expected = self.get_record_values("_id")
    #    assert data == expected

    def test_bool_column(self):
        data = self.get_monary_column("boolval", "bool")
        expected = self.get_record_values("boolval")
        assert data == expected

    # def test_date_column(self):
    #     column = self.get_monary_column("dateval", "date")
    #     expected = self.get_record_values("dateval")
    #     assert [monary.mongodate_to_datetime(x) for x in column] == expected
    #
    # def test_timestamp_column(self):
    #     raw_data = self.get_monary_column("timestampval", "timestamp")
    #     data = [struct.unpack("<ii", ts) for ts in raw_data]
    #     timestamps = self.get_record_values("timestampval")
    #     expected = [(ts.time, ts.inc) for ts in timestamps]
    #     assert data == expected
    #
    def test_string_column(self):
        data = self.get_monary_column("stringval", "string:5")
        expected = [s.encode('ascii')
                    for s in self.get_record_values("stringval")]
        assert data == expected

    # def test_binary_column(self):
    #     if PY3:
    #         # Python 3.
    #         data = [bytes(x)
    #                 for x in self.get_monary_column("binaryval", "binary:5")]
    #         expected = [bytes(b)[:5]
    #                     for b in self.get_record_values("binaryval")]
    #     else:
    #         # Python 2.6 / 2.7.
    #         data = [str(x)
    #                 for x in self.get_monary_column("binaryval", "binary:5")]
    #         expected = [str(b)
    #                     for b in self.get_record_values("binaryval")]
    #     assert data == expected

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
    #
    # def test_type_column(self):
    #     # See: http://bsonspec.org/#/specification for type codes.
    #     data = self.get_monary_column("floatval", "type")
    #     expected = [1] * len(data)
    #     assert data == expected
    #
    #     data = self.get_monary_column("stringval", "type")
    #     expected = [2] * len(data)
    #     assert data == expected
    #
    #     #data = self.get_monary_column("intlistval", "type")
    #     #expected = [4] * len(data)
    #     #assert data == expected
    #
    #     data = self.get_monary_column("binaryval", "type")
    #     expected = [5] * len(data)
    #     assert data == expected
    #
    #     data = self.get_monary_column("boolval", "type")
    #     expected = [8] * len(data)
    #     assert data == expected
    #
    #     data = self.get_monary_column("dateval", "type")
    #     expected = [9] * len(data)
    #     assert data == expected
    #
    #     data = self.get_monary_column("intval", "type")
    #     expected = [16] * len(data)
    #     assert data == expected
    #
    def test_string_length_column(self):
        data = self.get_monary_column("stringval", "length")
        expected = [len(x) for x in self.get_record_values("stringval")]
        assert data == expected

    # #def test_list_length_column(self):
    # #    data = self.get_monary_column("intlistval", "length")
    # #    expected = [len(x) for x in self.get_record_values("intlistval")]
    # #    assert data == expected
    #
    def test_bson_length_column(self):
        data = self.get_monary_column("subdocumentval", "length")
        # We have only one key in the subdocument.
        expected = [1] * len(data)
        assert data == expected

    def test_string_size_column(self):
        data = self.get_monary_column("stringval", "size")
        expected = [len(x) for x in self.get_record_values("stringval")]
        print "DATA=", data
        print "EXPECTED=", expected
        assert data == expected

    # #def test_list_size_column(self):
    # #    lists = self.get_record_values("intlistval")
    # #    data = self.get_monary_column("intlistval", "size")
    # #    expected = [len(bson.BSON.encode(self.list_to_bsonable_dict(l)))
    # #                for l in lists]
    # #    assert data == expected
    #
    def test_bson_size_column(self):
        data = self.get_monary_column("subdocumentval", "size")
        expected = [len(bson.BSON.encode(record))
                    for record in self.get_record_values("subdocumentval")]
        assert data == expected

    # def test_binary_size_column(self):
    #     data = self.get_monary_column("binaryval", "size")
    #     expected = [len(x) for x in self.get_record_values("binaryval")]
    #     assert data == expected
