import pymongo
import pymongo.timestamp
import bson
import monary
import random
import datetime

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

    random.seed(1234) # for reproducability

    for i in xrange(NUM_TEST_RECORDS):
        record = dict(
                    sequence=i,
                    intval=random.randint(-128, 127),
                    floatval=random.uniform(-1e30, 1e30),
                    boolval=(i % 2 == 0),
                    dateval=(datetime.datetime(1970, 1, 1) +
                             datetime.timedelta(days=random.randint(0, 60 * 365),
                                                seconds=random.randint(0, 24 * 60 * 60),
                                                milliseconds=random.randint(0, 1000))),
                    stringval="".join(chr(ord('A') + random.randint(0,25))
                                        for i in xrange(random.randint(1,5))),
                    timestampval=pymongo.timestamp.Timestamp(time=random.randint(0,1000000),
                                                             inc=random.randint(0,1000000)),
                )
        records.append(record)
    coll.insert(records, safe=True)
    RECORDS = records
    print "setup complete"
    
def teardown():
    c = get_pymongo_connection()
    c.drop_database("monary_test")
    print "teardown complete"

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

def check_float_column(coltype):
    data = get_monary_column("floatval", coltype)
    expected = get_record_values("floatval")
    assert data == expected

def test_int_columns():
    for coltype in ["int8", "int16", "int32", "int64"]:
        yield check_int_column, coltype

def test_float_columns():
    for coltype in ["float32", "float64"]:
        yield check_int_column, coltype

def test_id_column():
    column = get_monary_column("_id", "id")
    data = [ bson.ObjectId(str(c)) for c in column ]
    expected = get_record_values("_id")
    assert data == expected

def test_bool_column():
    data = get_monary_column("boolval", "bool")
    expected = get_record_values("boolval")
    assert data == expected

def test_date_column():
    column = get_monary_column("dateval", "date")
    data = [ monary.mongodate_to_datetime(val) for val in column ]
    expected = get_record_values("dateval")
    assert data == expected

def test_timestamp_column():
    data = get_monary_column("timestampval", "timestamp")
    timestamps = get_record_values("timestampval")
    expected = [ ((ts.time << 32) + ts.inc) for ts in timestamps ]
    assert data == expected

def test_string_column():
    data = get_monary_column("stringval", "string:5")
    expected = get_record_values("stringval")
    assert data == expected

def test_type_column():
    data = get_monary_column("dateval", "type")
    # Note: "9" is the bson type code for 'date' values
    # see: http://bsonspec.org/#/specification
    expected = [ 9 ] * len(data)
    assert data == expected

def test_string_length_column():
    data = get_monary_column("stringval", "length")
    expected = [ len(x) for x in get_record_values("stringval") ]
    assert data == expected
