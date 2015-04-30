import re

import bson
import pymongo

import monary
from test import db_err, unittest


@unittest.skipIf(db_err, db_err)
class TestExceptions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with pymongo.MongoClient() as c:
            c.drop_database("monary_test")

    @classmethod
    def tearDownClass(cls):
        with pymongo.MongoClient() as c:
            c.drop_database("monary_test")

    def test_get_monary_numpy_type1(self):
        with self.assertRaisesRegexp(
                ValueError,
                "Too many parts in type"):
            with monary.Monary() as m:
                m.query("test", "collection", {},
                        ["x1", "x2", "x3", "x4", "x5"], ["string:6:4"] * 5)

    def test_get_monary_numpy_type2(self):
        with self.assertRaisesRegexp(
                ValueError,
                "Unable to parse type argument"):
            with monary.Monary() as m:
                m.query("test", "collection", {},
                        ["x1", "x2", "x3", "x4", "x5"], ["string:x"] * 5)

    def test_get_monary_numpy_type3(self):
        with self.assertRaisesRegexp(
                ValueError,
                "Unknown typename"):
            with monary.Monary() as m:
                m.query("test", "collection", {},
                        ["x1", "x2", "x3", "x4", "x5"], ["junk"] * 5)

    def test_get_monary_numpy_type4(self):
        with self.assertRaisesRegexp(
                ValueError,
                "'string' must have an explicit typearg with nonzero length"):
            with monary.Monary() as m:
                m.query("test", "collection", {},
                        ["x1", "x2", "x3", "x4", "x5"], ["string:0"] * 5)

    def test_get_full_query1(self):
        with self.assertRaisesRegexp(
                ValueError,
                "Invalid ordering: should be str or list of \(column, "
                "direction\) pairs"):
            with monary.Monary() as m:
                m.query("test", "collection", {},
                        ["x1", "x2", "x3", "x4", "x5"], ["float64"] * 5,
                        sort=monary.Monary())

    def test_get_full_query2(self):
        with self.assertRaisesRegexp(
                ValueError,
                "Invalid ordering: should be str or list of \(column, "
                "direction\) pairs"):
            with monary.Monary() as m:
                m.query("test", "collection", {},
                        ["x1", "x2", "x3", "x4", "x5"], ["float64"] * 5,
                        hint=monary.Monary())

    def test_monary_connect3(self):
        with self.assertRaisesRegexp(
                ValueError,
                "You cannot have a password with no username"):
            with monary.Monary(password='mng') as m:
                m.query("test", "collection", {},
                        ["x1", "x2", "x3", "x4", "x5"], ["float64"] * 5)

    def test_make_column_data1(self):
        with self.assertRaisesRegexp(
                ValueError,
                "Number of fields and types do not match"):
            with monary.Monary() as m:
                m.query("test", "collection", {},
                        ["x1"], ["float64"] * 5)

    def test_make_column_data2(self):
        with self.assertRaisesRegexp(
                ValueError,
                "Number of fields exceeds maximum of 1024"):
            with monary.Monary() as m:
                m.query("test", "collection", {},
                        ["x1"] * 1025, ["float64"] * 1025)

    def test_make_column_data3(self):
        with self.assertRaisesRegexp(
                ValueError,
                "exceeds maximum of 1024"):
            with monary.Monary() as m:
                st = "x" * 1025
                m.query("test", "collection", {},
                        [st], ["float64"])

    def test_get_pipeline(self):
        with self.assertRaisesRegexp(
                TypeError,
                "Pipeline must be a dict or a list"):
            with monary.Monary() as m:
                m.aggregate("test", "collection", "this is not a list", {},
                            ['x1'], ["float64"])

    def test_monary_connect1(self):
        with self.assertRaisesRegexp(
                monary.monary.MonaryError,
                "Failed to resolve"):
            with monary.Monary('mongodb://asfadsf') as m:
                m.query("test", "collection", {},
                        ["x1", "x2", "x3", "x4", "x5"], ["float64"] * 5)

    def test_monary_connect2(self):
        with self.assertRaisesRegexp(
                monary.monary.MonaryError,
                re.compile("(Failed to authenticate credentials|Authentication"
                           " failed)")):
            with monary.Monary(username='mng') as m:
                m.query("test", "collection", {},
                        ["x1", "x2", "x3", "x4", "x5"], ["float64"] * 5)

    def test_monary_count1(self):
        with self.assertRaisesRegexp(
                monary.monary.MonaryError,
                "Invalid ns"):
            with monary.Monary() as m:
                m.query("", "collection", {},
                        ["x1", "x2", "x3", "x4", "x5"], ["float64"] * 5)

    def test_monary_count2(self):
        with self.assertRaisesRegexp(
                monary.monary.MonaryError,
                "Invalid ns \[not a db.\$cmd\]"):
            with monary.Monary() as m:
                m.count("not a db", "")

    def test_monary_count3(self):
        with self.assertRaisesRegexp(
                monary.monary.MonaryError,
                "Failed to handshake and validate TLS certificate"):
            with monary.Monary("mongodb://localhost:27017/?ssl=true") as m:
                m.query("test", "collection", {},
                        ["x1", "x2", "x3", "x4", "x5"], ["float64"] * 5)

    def test_monary_query_bson(self):
        # Test should fail in monary_init_query.
        with self.assertRaisesRegexp(
                bson.InvalidDocument,
                "documents must have only string keys, key was 0"):
            with monary.Monary() as m:
                m.query("test", "collection", {0: 0},
                        ["x1", "x2", "x3", "x4", "x5"], ["float64"] * 5)

    def test_monary_aggregate(self):
        # Test should fail in monary_load_query.
        with self.assertRaisesRegexp(
                monary.monary.MonaryError,
                "A pipeline stage specification object must contain exactly "
                "one field"):
            with monary.Monary() as m:
                m.aggregate("test", "collection", {},
                            ['x1', 'x2', 'x3', 'x4', 'x5'],
                            ["float64"] * 5)

    def test_monary_aggregate2(self):
        # Test should fail in monary_load_query.
        with self.assertRaisesRegexp(
                monary.monary.MonaryError,
                "exception: Unrecognized pipeline stage name:"):
            with monary.Monary() as m:
                m.aggregate("test", "collection", {"hi": "you"},
                            ['x1', 'x2', 'x3', 'x4', 'x5'],
                            ["float64"] * 5)
