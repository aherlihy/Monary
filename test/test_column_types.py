import pymongo
import bson
import monary
import random
import datetime

def get_pymongo_connection():
    return pymongo.Connection("127.0.0.1")

def get_monary_connection():
    return monary.Monary("127.0.0.1")

RECORDS = None

def setup():
    global RECORDS
    c = get_pymongo_connection()
    db = c.monary_test
    coll = db.test_data
    coll.remove({}, safe=True) # ensure collection is empty at start
    records = [ ]
    for i in range(-50, 50):
        record = dict(
                    seqintval=i,
                    seqfloatval=float(i * 0.5),
                    seqdateval=(datetime.datetime(2000, 1, 1) + datetime.timedelta(days=i)),
                    boolval=(i % 2 == 0),
                    randintval=random.randint(-10, 10),
                    randfloatval=random.uniform(-10.0, 10.0),
                )
        records.append(record)
    coll.insert(records, safe=True)
    RECORDS = records
    print "setup complete"
    
def teardown():
    c = get_pymongo_connection()
    db = c.monary_test
    db.drop_collection("test_data")
    print "teardown complete"

def check_int_column(coltype):
    with get_monary_connection() as m:
        [ column ] = m.query("monary_test", "test_data", {}, ["seqintval"], [coltype])
    assert list(column) == range(-50, 50)

def check_float_column(coltype):
    with get_monary_connection() as m:
        [ column ] = m.query("monary_test", "test_data", {}, ["seqfloatval"], [coltype])
    expected = [ float(x * 0.5) for x in range(-50, 50) ]
    assert list(column) == expected

def test_int_columns():
    for coltype in ["int8", "int16", "int32", "int64"]:
        yield check_int_column, coltype

def test_float_columns():
    for coltype in ["float32", "float64"]:
        yield check_int_column, coltype

def test_id_column():
    with get_monary_connection() as m:
        [ column ] = m.query("monary_test", "test_data", {}, ["_id"], ["id"])
    coldata = [ bson.ObjectId(str(c)) for c in column ]

    c = get_pymongo_connection()
    ids = [ rec["_id"] for rec in c.monary_test.test_data.find() ]
    assert coldata == ids

def test_bool_column():
    with get_monary_connection() as m:
        [ column ] = m.query("monary_test", "test_data", {}, ["boolval"], ["bool"])
    assert list(column) == [True, False] * 50

def test_date_column():
    with get_monary_connection() as m:
        [ column ] = m.query("monary_test", "test_data", {}, ["seqdateval"], ["date"])
    expected = [ monary.datetime_to_mongodate(record["seqdateval"]) for record in RECORDS ]
    assert list(column) == expected
