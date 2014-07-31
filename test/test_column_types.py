# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import random
import datetime
import struct
import sys

import bson
import numpy
import pymongo

import monary
from monary.monary import OrderedDict, mvoid_to_bson_id

PY3 = sys.version_info[0] >= 3
if PY3:
    xrange = range

NUM_TEST_RECORDS = 100

def get_pymongo_connection():
    return pymongo.Connection("127.0.0.1")

def get_monary_connection():
    return monary.Monary("127.0.0.1")

RECORDS = None

def setup():
    global RECORDS
    c = get_pymongo_connection()
    c.drop_database("monary_test") # ensure that database does not exist
    db = c.monary_test
    coll = db.test_data
    records = [ ]

    random.seed(1234) # for reproducibility

    for i in xrange(NUM_TEST_RECORDS):
        if PY3:
            # Python 3
            binary = "".join(chr(random.randint(0, 255)) for i in xrange(5))
            binary = binary.encode('utf-8')
        else:
            # Python 2.6 / 2.7
            binary = "".join(chr(random.randint(0, 255)) for i in xrange(5))
        record = dict(
                    sequence=i,
                    intval=random.randint(-128, 127),
                    uintval=random.randint(0, 255),
                    floatval=random.uniform(-1e30, 1e30),
                    boolval=(i % 2 == 0),
                    dateval=(datetime.datetime(1970, 1, 1) + (1 - 2 * random.randint(0,1)) *
                             datetime.timedelta(days=random.randint(0, 60 * 365),
                                                seconds=random.randint(0, 24 * 60 * 60),
                                                milliseconds=random.randint(0, 1000))),
                    timestampval=bson.timestamp.Timestamp(time=random.randint(0,1000000),
                                                          inc=random.randint(0,1000000)),
                    stringval="".join(chr(ord('A') + random.randint(0,25))
                                        for i in xrange(random.randint(1,5))),
                    binaryval=bson.binary.Binary(binary),
                    intlistval=[ random.randint(0, 100) for i in xrange(random.randint(1,5)) ],
                    subdocumentval=dict(subkey=random.randint(0, 255))
                )
        records.append(record)
    coll.insert(records, safe=True)
    RECORDS = records
    print("setup complete")

def teardown():
    c = get_pymongo_connection()
    c.drop_database("monary_test")
    print("teardown complete")

def get_record_values(colname):
    return [ r[colname] for r in RECORDS ]

def get_monary_column(colname, coltype):
    with get_monary_connection() as m:
        [ column ] = m.query("monary_test", "test_data", {}, [colname], [coltype], sort="sequence")
    return list(column)

def check_int_column(coltype):
    data = get_monary_column("intval", coltype)
    expected = get_record_values("intval")
    assert data == expected

def check_uint_column(coltype):
    data = get_monary_column("uintval", coltype)
    expected = get_record_values("uintval")
    assert data == expected

def check_float_column(coltype):
    data = get_monary_column("floatval", coltype)
    expected = get_record_values("floatval")
    for d, e in zip(data, expected):
        numpy.testing.assert_almost_equal((d-e)/max(d,e), 0, decimal=5)

def test_int_columns():
    for coltype in ["int8", "int16", "int32", "int64"]:
        yield check_int_column, coltype
    for coltype in ["uint8", "uint16", "uint32", "uint64"]:
        yield check_uint_column, coltype

def test_float_columns():
    for coltype in ["float32", "float64"]:
        yield check_float_column, coltype

def test_id_column():
    column = get_monary_column("_id", "id")
    data = list(map(mvoid_to_bson_id, column))
    expected = get_record_values("_id")
    assert data == expected

def test_bool_column():
    data = get_monary_column("boolval", "bool")
    expected = get_record_values("boolval")
    assert data == expected

def test_date_column():
    column = get_monary_column("dateval", "date")
    expected = get_record_values("dateval")
    assert [monary.mongodate_to_datetime(x) for x in column] == expected

def test_timestamp_column():
    raw_data = get_monary_column("timestampval", "timestamp")
    data = [ struct.unpack("<ii", ts) for ts in raw_data ]
    timestamps = get_record_values("timestampval")
    expected = [ (ts.time, ts.inc) for ts in timestamps ]
    assert data == expected

def test_string_column():
    data = get_monary_column("stringval", "string:5")
    expected = [s.encode('ascii') for s in get_record_values("stringval")]
    assert data == expected

def test_binary_column():
    if PY3:
        # Python 3
        data = [bytes(x) for x in get_monary_column("binaryval", "binary:5")]
        expected = [bytes(b)[:5] for b in get_record_values("binaryval")]
    else:
        # Python 2.6 / 2.7
        data = [str(x) for x in get_monary_column("binaryval", "binary:5")]
        expected = [str(b) for b in get_record_values("binaryval")]
    assert data == expected

def test_nested_field():
    data = get_monary_column("subdocumentval.subkey", "int32")
    expected = [ r["subkey"] for r in get_record_values("subdocumentval") ]
    assert data == expected

def list_to_bsonable_dict(values):
    return OrderedDict((str(i), val) for i, val in enumerate(values))

def test_bson_column():
    size = max(get_monary_column("subdocumentval", "size"))
    rawdata = get_monary_column("subdocumentval", "bson:%d" % size)
    expected = get_record_values("subdocumentval")
    data = [bson.BSON(x).decode() for x in rawdata]
    assert data == expected

def test_type_column():
    # See: http://bsonspec.org/#/specification for type codes
    data = get_monary_column("floatval", "type")
    expected = [1] * len(data)
    assert data == expected

    data = get_monary_column("stringval", "type")
    expected = [2] * len(data)
    assert data == expected

    data = get_monary_column("intlistval", "type")
    expected = [4] * len(data)
    assert data == expected

    data = get_monary_column("binaryval", "type")
    expected = [5] * len(data)
    assert data == expected

    data = get_monary_column("boolval", "type")
    expected = [8] * len(data)
    assert data == expected

    data = get_monary_column("dateval", "type")
    expected = [9] * len(data)
    assert data == expected

    data = get_monary_column("intval", "type")
    expected = [16] * len(data)
    assert data == expected

def test_string_length_column():
    data = get_monary_column("stringval", "length")
    expected = [ len(x) for x in get_record_values("stringval") ]
    assert data == expected

def test_list_length_column():
    data = get_monary_column("intlistval", "length")
    expected = [ len(x) for x in get_record_values("intlistval") ]
    assert data == expected

def test_bson_length_column():
    data = get_monary_column("subdocumentval", "length")
    # We have only one key in the subdocument
    expected = [1] * len(data)
    assert data == expected

def test_string_size_column():
    data = get_monary_column("stringval", "size")
    expected = [ len(x) + 1 for x in get_record_values("stringval") ]
    assert data == expected

def test_list_size_column():
    lists = get_record_values("intlistval")
    data = get_monary_column("intlistval", "size")
    expected = [ len(bson.BSON.encode(list_to_bsonable_dict(list))) for list in lists ]
    assert data == expected

def test_bson_size_column():
    data = get_monary_column("subdocumentval", "size")
    expected = [ len(bson.BSON.encode(record)) for record in get_record_values("subdocumentval")]
    assert data == expected

def test_binary_size_column():
    data = get_monary_column("binaryval", "size")
    expected = [ len(x) for x in get_record_values("binaryval") ]
    assert data == expected
