import os

import nose
import pymongo

from test import IntegrationTest, unittest

import monary

"""Test file for SSL functionality of monary.

See test_ssl_instructions.txt.

"""


class TestSSLCert(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cert_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'certificates')
        cls.client_pem = os.path.join(cls.cert_path, 'client.pem')
        cls.ca_pem = os.path.join(cls.cert_path, 'ca.pem')

        try:
            client = pymongo.MongoClient("mongodb://localhost:27017/?ssl=true",
                                         ssl=True, ssl_certfile=cls.client_pem,
                                         ssl_ca_certs=cls.ca_pem)
            collection = client.test.ssl
            collection.drop()
            collection.insert({'x1': 0.0})
        except pymongo.errors.ConnectionFailure as e:
            if "SSL handshake failed" in str(e):
                raise nose.SkipTest("Can't connect to mongod with SSL: " +
                                    e.message)
            else:
                raise Exception("Unable to connect to mongod: ", str(e))

    def test_all(self):
        with monary.Monary("mongodb://localhost:27017/?ssl=true",
                           pem_file=self.client_pem,
                           ca_file=self.ca_pem,
                           ca_dir=self.cert_path,
                           weak_cert_validation=False) as m:
            arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
            assert len(arrays) == 1 and arrays[0] == 0.0

    def test_pem(self):
        with monary.Monary("mongodb://localhost:27017/?ssl=true",
                           pem_file=self.client_pem,
                           weak_cert_validation=True) as m:
            arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
            assert len(arrays) == 1 and arrays[0] == 0.0

    def test_uri(self):
        with monary.Monary("mongodb://localhost:27017/?ssl=true",
                           pem_file=self.client_pem,
                           ca_file=self.ca_pem,
                           ca_dir=self.cert_path,
                           weak_cert_validation=True) as m:
            arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
            assert len(arrays) == 1 and arrays[0] == 0.0

    def test_bad_uri(self):
        with self.assertRaisesRegexp(monary.monary.MonaryError,
                                     "Failed to read 4 bytes from socket."):
            with monary.Monary("mongodb://localhost:27017",
                               pem_file=self.client_pem,
                               ca_file=self.ca_pem,
                               ca_dir=self.cert_path,
                               weak_cert_validation=True) as m:
                arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
                assert len(arrays) == 1 and arrays[0] == 0.0

    def test_ssl_false(self):
        with self.assertRaisesRegexp(monary.monary.MonaryError,
                                     "Failed to read 4 bytes from socket."):
            with monary.Monary("mongodb://localhost:27017/?ssl=false",
                               pem_file=self.client_pem) as m:
                arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
            assert len(arrays) == 1 and arrays[0] == 0.0

    def test_validate_server_cert(self):
        with monary.Monary("mongodb://localhost:27017/?ssl=true",
                           pem_file=self.client_pem,
                           ca_file=self.ca_pem,
                           weak_cert_validation=False) as m:
            arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
            assert len(arrays) == 1 and arrays[0] == 0.0


class TestNoSSL(IntegrationTest):

    @classmethod
    def setUpClass(cls):
        super(TestNoSSL, cls).setUpClass()

        cls.cert_path = os.path.join(os.path.dirname(
            os.path.realpath(__file__)),
            'certificates')
        cls.client_pem = os.path.join(cls.cert_path, 'client.pem')
        cls.ca_pem = os.path.join(cls.cert_path, 'ca.pem')

        with pymongo.MongoClient() as client:
            collection = client.test.ssl
            collection.drop()
            collection.insert({'x1': 0.0})

    def test_ssl_false_no_ssl(self):
        with monary.Monary("mongodb://localhost:27017/?ssl=false",
                           pem_file=self.client_pem) as m:
            arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
            assert len(arrays) == 1 and arrays[0] == 0.0

    def test_bad_uri_no_ssl(self):
        with monary.Monary("mongodb://localhost:27017",
                           pem_file=self.client_pem,
                           ca_file=self.ca_pem,
                           ca_dir=self.cert_path,
                           weak_cert_validation=True) as m:
            arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
            assert len(arrays) == 1 and arrays[0] == 0.0
