import os

from nose import SkipTest
import pymongo

import monary
import test_helpers

"""
Test file for SSL functionality of monary.

See test_ssl_instructions.txt

"""


def inittest_cert():
    cert_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             'certificates')
    client_pem = os.path.join(cert_path, 'client.pem')
    ca_pem = os.path.join(cert_path, 'ca.pem')
    return cert_path, client_pem, ca_pem


def inittest_ssl():
    cert_path, client_pem, ca_pem = inittest_cert()
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017/?ssl=true",
                                     ssl=True, ssl_certfile=client_pem,
                                     ssl_ca_certs=ca_pem)
        collection = client.test.ssl
        collection.drop()
        collection.insert({'x1': 0.0})
    except pymongo.errors.ConnectionFailure as e:
        if "SSL handshake failed" in str(e):
            raise SkipTest("Can't connect to mongod with SSL", str(e))
        else:
            raise Exception("Unable to connect to mongod: ", str(e))
    return cert_path, client_pem, ca_pem


def inittest_no_ssl():
    cert_path, client_pem, ca_pem = inittest_cert()
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017")
    except pymongo.errors.ConnectionFailure as e:
        raise SkipTest("Non-SSL connection failed", str(e))
    else:
        collection = client.test.ssl
        collection.drop()
        collection.insert({'x1': 0.0})
    return cert_path, client_pem, ca_pem


def query_with(monary):
    arrays = monary.query("test", "ssl", {}, ["x1"], ["float64"])
    assert len(arrays) == 1 and arrays[0] == 0.0


def test_all():
    cert_path, client_pem, ca_pem = inittest_ssl()
    with monary.Monary("mongodb://localhost:27017/?ssl=true",
                       pem_file=client_pem,
                       ca_file=ca_pem,
                       ca_dir=cert_path,
                       weak_cert_validation=False) as m:
        query_with(m)


def test_pem():
    cert_path, client_pem, ca_pem = inittest_ssl()
    with monary.Monary("mongodb://localhost:27017/?ssl=true",
                       pem_file=client_pem,
                       weak_cert_validation=True) as m:
        query_with(m)


def test_uri():
    cert_path, client_pem, ca_pem = inittest_ssl()
    with monary.Monary("mongodb://localhost:27017/?ssl=true",
                       pem_file=client_pem,
                       ca_file=ca_pem,
                       ca_dir=cert_path,
                       weak_cert_validation=True) as m:
        query_with(m)


def test_bad_uri():
    cert_path, client_pem, ca_pem = inittest_ssl()
    with test_helpers.assertraises(monary.monary.MonaryError,
                                   "Failed to read 4 bytes from socket."):
        with monary.Monary("mongodb://localhost:27017",
                           pem_file=client_pem,
                           ca_file=ca_pem,
                           ca_dir=cert_path,
                           weak_cert_validation=True) as m:
            query_with(m)


def test_ssl_false_no_ssl():
    client_path, client_pem, ca_pem = inittest_no_ssl()
    with monary.Monary("mongodb://localhost:27017/?ssl=false",
                       pem_file=client_pem) as m:
        query_with(m)


def test_bad_uri_no_ssl():
    cert_path, client_pem, ca_pem = inittest_no_ssl()
    with monary.Monary("mongodb://localhost:27017",
                       pem_file=client_pem,
                       ca_file=ca_pem,
                       ca_dir=cert_path,
                       weak_cert_validation=True) as m:
        query_with(m)


def test_ssl_false():
    cert_path, client_pem, ca_pem = inittest_ssl()
    with test_helpers.assertraises(monary.monary.MonaryError,
                                   "Failed to read 4 bytes from socket."):
        with monary.Monary("mongodb://localhost:27017/?ssl=false",
                           pem_file=client_pem) as m:
            query_with(m)


def test_validate_server_cert():
    cert_path, client_pem, ca_pem = inittest_ssl()
    with monary.Monary("mongodb://localhost:27017/?ssl=true",
                       pem_file=client_pem,
                       ca_file=ca_pem,
                       weak_cert_validation=False) as m:
        query_with(m)


