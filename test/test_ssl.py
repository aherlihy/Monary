import os

import pymongo

import monary
from test import db_err, unittest

"""Test file for SSL functionality of monary.

See test_ssl_instructions.txt.

"""

# If there was an error connecting, see if SSL is on.
ssl_err = ""
global_cert_path = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'certificates')
global_client_pem = os.path.join(global_cert_path, 'client.pem')
global_ca_pem = os.path.join(global_cert_path, 'ca.pem')

try:
    with pymongo.MongoClient("mongodb://localhost:27017/?ssl=true",
                             ssl=True,
                             ssl_certfile=global_client_pem,
                             ssl_ca_certs=global_ca_pem) as global_client:
        global_coll = global_client.test.ssl
        global_coll.drop()
        global_coll.insert({'x1': 0.0})
except pymongo.errors.ConnectionFailure as globl_e:
    if ("SSL handshake failed" in str(
            globl_e) or "forcibly closed by the remote host" in str(globl_e)):
        ssl_err = "Can't connect to mongod with SSL: " + str(globl_e)
    else:
        raise RuntimeError("Unable to connect to mongod: ", str(globl_e))


@unittest.skipIf(ssl_err, ssl_err)
class TestSSLCert(unittest.TestCase):

    @classmethod
    def tearDownClass(cls):
        with pymongo.MongoClient("mongodb://localhost:27017/?ssl=true",
                                 ssl=True,
                                 ssl_certfile=global_client_pem,
                                 ssl_ca_certs=global_ca_pem) as c:
            c.drop_database("test")

    def test_all(self):
        with monary.Monary("mongodb://localhost:27017/?ssl=true",
                           pem_file=global_client_pem,
                           ca_file=global_ca_pem,
                           ca_dir=global_cert_path,
                           weak_cert_validation=False) as m:
            arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
            assert len(arrays) == 1 and arrays[0] == 0.0

    def test_pem(self):
        with monary.Monary("mongodb://localhost:27017/?ssl=true",
                           pem_file=global_client_pem,
                           weak_cert_validation=True) as m:
            arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
            assert len(arrays) == 1 and arrays[0] == 0.0

    def test_uri(self):
        with monary.Monary("mongodb://localhost:27017/?ssl=true",
                           pem_file=global_client_pem,
                           ca_file=global_ca_pem,
                           ca_dir=global_cert_path,
                           weak_cert_validation=True) as m:
            arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
            assert len(arrays) == 1 and arrays[0] == 0.0

    def test_bad_uri(self):
        with self.assertRaisesRegexp(monary.monary.MonaryError,
                                     "Failed to read 4 bytes from socket."):
            with monary.Monary("mongodb://localhost:27017",
                               pem_file=global_client_pem,
                               ca_file=global_ca_pem,
                               ca_dir=global_cert_path,
                               weak_cert_validation=True) as m:
                arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
                assert len(arrays) == 1 and arrays[0] == 0.0

    def test_ssl_false(self):
        with self.assertRaisesRegexp(monary.monary.MonaryError,
                                     "Failed to read 4 bytes from socket."):
            with monary.Monary("mongodb://localhost:27017/?ssl=false",
                               pem_file=global_client_pem) as m:
                arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
            assert len(arrays) == 1 and arrays[0] == 0.0

    def test_validate_server_cert(self):
        with monary.Monary("mongodb://localhost:27017/?ssl=true",
                           pem_file=global_client_pem,
                           ca_file=global_ca_pem,
                           weak_cert_validation=False) as m:
            arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
            assert len(arrays) == 1 and arrays[0] == 0.0


@unittest.skipIf(db_err, db_err)
class TestNoSSL(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with pymongo.MongoClient() as client:
            collection = client.test.ssl
            collection.drop()
            collection.insert({'x1': 0.0})

    @classmethod
    def tearDownClass(cls):
        with pymongo.MongoClient() as c:
            c.drop_database("test")

    def test_ssl_false_no_ssl(self):
        with monary.Monary("mongodb://localhost:27017/?ssl=false",
                           pem_file=global_client_pem) as m:
            arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
            assert len(arrays) == 1 and arrays[0] == 0.0

    def test_bad_uri_no_ssl(self):
        with monary.Monary("mongodb://localhost:27017",
                           pem_file=global_client_pem,
                           ca_file=global_ca_pem,
                           ca_dir=global_cert_path,
                           weak_cert_validation=True) as m:
            arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
            assert len(arrays) == 1 and arrays[0] == 0.0
