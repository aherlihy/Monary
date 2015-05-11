import os

import numpy as np

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
    with monary.Monary("mongodb://localhost:27017/?ssl=true",
                       pem_file=global_client_pem,
                       ca_file=global_ca_pem,
                       ca_dir=global_cert_path,
                       weak_cert_validation=False) as mnr:
        mnr.drop_collection("test", "ssl")
        gparam = monary.MonaryParam.from_lists(
            [np.ma.masked_array([0], [0], "float64")], ["x1"], ["float64"])
        mnr.insert("test", "ssl", gparam)
except monary.monary.MonaryError as globl_e:
    if "Failed to handshake and validate TLS certificate" in str(globl_e):
        ssl_err = "Can't connect to mongod with SSL: " + str(globl_e)
    else:
        raise RuntimeError("Unable to connect to mongod: ", str(globl_e))


@unittest.skipIf(ssl_err, ssl_err)
class TestSSLCert(unittest.TestCase):

    @classmethod
    def tearDownClass(cls):
        with monary.Monary("mongodb://localhost:27017/?ssl=true",
                           pem_file=global_client_pem,
                           ca_file=global_ca_pem,
                           ca_dir=global_cert_path,
                           weak_cert_validation=False) as m:
            m.drop_collection("test", "ssl")

    def test_all(self):
        with monary.Monary("mongodb://localhost:27017/?ssl=true",
                           pem_file=global_client_pem,
                           ca_file=global_ca_pem,
                           ca_dir=global_cert_path,
                           weak_cert_validation=False) as m:
            arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
            self.assertEqual(len(arrays), 1)
            self.assertEqual(arrays[0], 0.0)

    def test_pem(self):
        with monary.Monary("mongodb://localhost:27017/?ssl=true",
                           pem_file=global_client_pem,
                           weak_cert_validation=True) as m:
            arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
            self.assertEqual(len(arrays), 1)
            self.assertEqual(arrays[0], 0.0)

    def test_uri(self):
        with monary.Monary("mongodb://localhost:27017/?ssl=true",
                           pem_file=global_client_pem,
                           ca_file=global_ca_pem,
                           ca_dir=global_cert_path,
                           weak_cert_validation=True) as m:
            arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
            self.assertEqual(len(arrays), 1)
            self.assertEqual(arrays[0], 0.0)

    def test_bad_uri(self):
        with self.assertRaisesRegexp(monary.monary.MonaryError,
                                     "Failed to read 4 bytes from socket."):
            with monary.Monary("mongodb://localhost:27017",
                               pem_file=global_client_pem,
                               ca_file=global_ca_pem,
                               ca_dir=global_cert_path,
                               weak_cert_validation=True) as m:
                arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
                self.assertEqual(len(arrays), 1)
                self.assertEqual(arrays[0], 0.0)

    def test_ssl_false(self):
        with self.assertRaisesRegexp(monary.monary.MonaryError,
                                     "Failed to read 4 bytes from socket."):
            with monary.Monary("mongodb://localhost:27017/?ssl=false",
                               pem_file=global_client_pem) as m:
                arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
            self.assertEqual(len(arrays), 1)
            self.assertEqual(arrays[0], 0.0)

    def test_validate_server_cert(self):
        with monary.Monary("mongodb://localhost:27017/?ssl=true",
                           pem_file=global_client_pem,
                           ca_file=global_ca_pem,
                           weak_cert_validation=False) as m:
            arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
            self.assertEqual(len(arrays), 1)
            self.assertEqual(arrays[0], 0.0)


@unittest.skipIf(db_err, db_err)
class TestNoSSL(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with monary.Monary() as m:
            m.drop_collection("test", "ssl")
            param = monary.MonaryParam.from_lists(
                [np.ma.masked_array([0], [0], "float64")], ["x1"], ["float64"])
            m.insert("test", "ssl", param)

    @classmethod
    def tearDownClass(cls):
        with monary.Monary() as m:
            m.drop_collection("test", "ssl")

    def test_ssl_false_no_ssl(self):
        with monary.Monary("mongodb://localhost:27017/?ssl=false",
                           pem_file=global_client_pem) as m:
            arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
            self.assertEqual(len(arrays), 1)
            self.assertEqual(arrays[0], 0.0)

    def test_bad_uri_no_ssl(self):
        with monary.Monary("mongodb://localhost:27017",
                           pem_file=global_client_pem,
                           ca_file=global_ca_pem,
                           ca_dir=global_cert_path,
                           weak_cert_validation=True) as m:
            arrays = m.query("test", "ssl", {}, ["x1"], ["float64"])
            self.assertEqual(len(arrays), 1)
            self.assertEqual(arrays[0], 0.0)
