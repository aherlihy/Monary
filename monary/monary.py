# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import atexit
import copy
import ctypes
import os
import platform
import sys

PY3 = sys.version_info[0] >= 3
if PY3:
    # Python 3.
    bytes_type = bytes
    string_type = str
    from urllib.parse import urlencode
else:
    # Python 2.6/2.7.
    bytes_type = basestring
    string_type = basestring
    from urllib import urlencode

try:
    # if we are using Python 2.7+.
    from collections import OrderedDict
except ImportError:
    # for Python 2.6 and earlier.
    from .ordereddict import OrderedDict

import numpy
import bson

from .write_concern import WriteConcern

cmonary = None

ERROR_LEN = 504
ERROR_ARR = ctypes.c_char * ERROR_LEN


class bson_error_t(ctypes.Structure):
    _fields_ = [
        ("domain", ctypes.c_uint),
        ("code", ctypes.c_uint),
        ("message", ERROR_ARR)
    ]


def get_empty_bson_error():
    return bson_error_t(0, 0, "".encode("utf-8"))


class MonaryError(Exception):
    pass


def _load_cmonary_lib():
    """Loads the cmonary CDLL library (from the directory containing
    this module).
    """
    global cmonary
    moduledir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if platform.system() == 'Windows':
        cmonary_fname = "libcmonary.pyd"
    else:
        cmonary_fname = "libcmonary.so"

    cmonaryfile = None

    for root, dirs, files in os.walk(moduledir):
        for basename in files:
            if basename == cmonary_fname:
                cmonaryfile = os.path.join(root, basename)
                break
    if cmonaryfile is None:
        raise RuntimeError("Unable to find cmonary shared library: ",
                           cmonary_fname)

    cmonary = ctypes.CDLL(cmonaryfile)

_load_cmonary_lib()

CTYPE_CODES = {
    "P": ctypes.c_void_p,    # Pointer
    "S": ctypes.c_char_p,    # String
    "I": ctypes.c_int,       # Int
    "U": ctypes.c_uint,      # Unsigned int
    "L": ctypes.c_long,      # Long
    "B": ctypes.c_bool,      # Bool
    "0": None,        # None/void
}

# List of C function definitions from the cmonary library.
FUNCDEFS = [
    # Format: "func_name:arg_types:return_type".
    "monary_init::0",
    "monary_cleanup::0",
    "monary_connect:SP:P",
    "monary_disconnect:P:0",
    "monary_use_collection:PSS:P",
    "monary_destroy_collection:P:0",
    "monary_alloc_column_data:UU:P",
    "monary_free_column_data:P:I",
    "monary_set_column_item:PUSUUPPP:I",
    "monary_query_count:PPP:L",
    "monary_init_query:PUUPPIP:P",
    "monary_init_aggregate:PPPP:P",
    "monary_load_query:PP:I",
    "monary_close_query:P:0",
    "monary_create_write_concern:IIBBS:P",
    "monary_destroy_write_concern:P:0",
    "monary_insert:PPPPPP:0"
]

MAX_COLUMNS = 1024
MAX_STRING_LENGTH = 1024


def _decorate_cmonary_functions():
    """Decorates each of the cmonary functions with their argument and
    result types.
    """
    for funcdef in FUNCDEFS:
        name, argtypes, restype = funcdef.split(":")
        func = getattr(cmonary, name)
        func.argtypes = [CTYPE_CODES[c] for c in argtypes]
        func.restype = CTYPE_CODES[restype]

_decorate_cmonary_functions()

# Initialize Monary and register the cleanup function.
cmonary.monary_init()
atexit.register(cmonary.monary_cleanup)

# Table of type names and conversions between cmonary and numpy types.
MONARY_TYPES = {
    # "common_name": (cmonary_type_code, numpy_type_object)
    "id":        (1, numpy.dtype("<V12")),
    "bool":      (2, numpy.bool),
    "int8":      (3, numpy.int8),
    "int16":     (4, numpy.int16),
    "int32":     (5, numpy.int32),
    "int64":     (6, numpy.int64),
    "uint8":     (7, numpy.uint8),
    "uint16":    (8, numpy.uint16),
    "uint32":    (9, numpy.uint32),
    "uint64":    (10, numpy.uint64),
    "float32":   (11, numpy.float32),
    "float64":   (12, numpy.float64),
    "date":      (13, numpy.int64),
    "timestamp": (14, numpy.uint64),
    # Note, numpy strings do not need the null character.
    "string":    (15, "S"),
    # Raw data (void pointer).
    "binary":    (16, "<V"),
    "bson":      (17, "<V"),
    "type":      (18, numpy.uint8),
    "size":      (19, numpy.uint32),
    "length":    (20, numpy.uint32),
}


def get_monary_numpy_type(orig_typename):
    """Given a common typename, find the corresponding cmonary type number,
       type argument, and numpy type object (or code).

       The input typename must be one of the keys found in the ``MONARY_TYPES``
       dictionary.  These are common BSON type names such as ``id``, ``bool``,
       ``int32``, ``float64``, ``date``, or ``string``.
       If the type is ``string``,``binary``, or ``bson``, its name must be
       followed by a ``:size`` suffix indicating the maximum number of bytes
       that will be used to store the representation.

       :param str orig_typename: a common type name with optional argument
                                 (for fields with a size)
       :returns: (type_num, type_arg, numpy_type)
       :rtype: tuple
    """
    # Process any type_arg that might be included.
    if ':' in orig_typename:
        vals = orig_typename.split(':', 2)
        if len(vals) > 2:
            raise ValueError("Too many parts in type: %r" % orig_typename)
        type_name, arg = vals
        try:
            type_arg = int(arg)
        except ValueError:
            raise ValueError("Unable to parse type argument in: %r"
                             % orig_typename)
    else:
        type_arg = 0
        type_name = orig_typename

    if type_name not in MONARY_TYPES:
        raise ValueError("Unknown typename: %r" % type_name)
    if type_name in ("string", "binary", "bson"):
        if type_arg == 0:
            raise ValueError("%r must have an explicit typearg with nonzero "
                             "length (use 'string:20', for example)"
                             % type_name)
        type_num, numpy_type_code = MONARY_TYPES[type_name]
        numpy_type = numpy.dtype("%s%i" % (numpy_type_code, type_arg))
    else:
        type_num, numpy_type = MONARY_TYPES[type_name]
    return type_num, type_arg, numpy_type


def make_bson(obj):
    """Given a Python (JSON compatible) dictionary, returns a BSON string.

       (This hijacks the Python -> BSON conversion code from pymongo,
       which is needed for converting queries.  Perhaps this dependency
       can be removed in a later version.)

       :param obj: object to be encoded as BSON (dict, string, or None)
       :returns: BSON encoded representation (byte string)
       :rtype: str
    """
    if obj is None:
        obj = {}
    if not isinstance(obj, bytes_type):
        obj = bson.BSON.encode(obj)
    return obj


def mvoid_to_bson_id(mvoid):
    """Converts a numpy mvoid value to a BSON ObjectId.

       :param mvoid: numpy.ma.core.mvoid returned from Monary
       :returns: the _id as a bson ObjectId
       :rtype: bson.objectid.ObjectId
    """
    if PY3:
        # Python 3.
        string = str(mvoid)
        string_list = ''.join(filter(lambda y: y not in '[]', string)).split()
        ints = map(int, string_list)
        uints = [x & 0xff for x in ints]
        id_bytes = bytes(uints)
        return bson.ObjectId(id_bytes)
    else:
        # Python 2.6 / 2.7.
        return bson.ObjectId(str(mvoid))


def validate_insert_fields(fields):
    """Validate field names for insertion.

       :param fields: a list of field names

       :returns: None
    """
    for f in fields:
        if f.endswith('.'):
            raise ValueError("invalid fieldname: %r, must not end in '.'" % f)
        if '$' in f:
            raise ValueError("invalid fieldname: %r, must not contain '$'" % f)

    if len(fields) != len(set(fields)):
        raise ValueError("field names must all be unique")

    for f1 in fields:
        for f2 in fields:
            if f1 != f2 and f1.startswith(f2) and f1[len(f2)] == '.':
                raise ValueError("fieldname %r conflicts with nested-document "
                                 "fieldname %r" % (f2, f1))


def get_ordering_dict(obj):
    """Converts a field/direction specification to an OrderedDict, suitable
       for BSON encoding.

       :param obj: single field name or list of (field, direction) pairs
       :returns: mapping representing the field/direction list
       :rtype: OrderedDict
    """
    if obj is None:
        return OrderedDict()
    elif isinstance(obj, string_type):
        return OrderedDict([(obj, 1)])
    elif isinstance(obj, list):
        return OrderedDict(obj)
    else:
        raise ValueError("Invalid ordering: should be str or list of "
                         "(column, direction) pairs")


def get_plain_query(query):
    """Composes a plain query from the given query object.

       :param dict query: query dictionary (or None)
       :returns: BSON encoded query (byte string)
       :rtype: str
    """
    if query is None:
        query = {}
    return make_bson(query)


def get_full_query(query, sort=None, hint=None):
    """Composes a full query from the given query object, and sort and hint
    clauses, if provided.

     :param dict query: query dictionary (or None)
     :param sort: (optional) single field name or list of (field, direction)
                  pairs
     :param hint: (optional) single field name or list of (field, direction)
                  pairs
     :returns: BSON encoded query (byte string)
     :rtype: str
    """
    if query is None:
        query = {}

    if sort or hint:
        query = OrderedDict([("$query", query)])
        if sort:
            query["$orderby"] = get_ordering_dict(sort)
        if hint:
            query["$hint"] = get_ordering_dict(hint)

    return make_bson(query)


def get_pipeline(pipeline):
    """Manipulates the input pipeline into a usable form."""
    if isinstance(pipeline, list):
        pipeline = {"pipeline": pipeline}
    elif isinstance(pipeline, dict):
        if "pipeline" not in pipeline:
            pipeline = {"pipeline": [pipeline]}
    else:
        raise TypeError("Pipeline must be a dict or a list")
    return pipeline


class Monary(object):
    """Represents a 'monary' connection to a particular MongoDB server."""

    def __init__(self, host="localhost", port=27017, username=None,
                 password=None, database=None, pem_file=None,
                 pem_pwd=None, ca_file=None, ca_dir=None, crl_file=None,
                 weak_cert_validation=True, options=None):
        """

            An example of initializing monary with a port and hostname:
            >>> m = Monary("localhost", 27017)
            An example of initializing monary with a URI and SSL parameters:
            >>> m = Monary("mongodb://localhost:27017/?ssl=true",
            ...             pem_file='client.pem', ca_file='ca.pem',
            ...             crl_file='crl.pem')


           :param host: either host name (or IP) to connect to, or full URI
           :param port: port number of running MongoDB service on host
           :param username: An optional username for authentication.
           :param password: An optional password for authentication.
           :param database: The database to authenticate to if the URI
           specifies a username and password. If this is not specified but
           credentials exist, this defaults to the "admin" database. See
           mongoc_uri(7).
           :param pem_file: SSL certificate and key file
           :param pem_pwd: Passphrase for encrypted key file
           :param ca_file: Certificate authority file
           :param ca_dir: Directory for certificate authority files
           :param crl_file: Certificate revocation list file
           :param weak_cert_validation: bypass validation
           :param options: Connection-specific options as a dict.
        """

        self._cmonary = cmonary
        self._connection = None
        self.connect(host, port, username, password, database,
                     pem_file, pem_pwd, ca_file, ca_dir, crl_file,
                     weak_cert_validation, options)

    def connect(self, host="localhost", port=27017, username=None,
                password=None, database=None, p_file=None,
                pem_pwd=None, ca_file=None, ca_dir=None, c_file=None,
                weak_cert_validation=False, options=None):
        """Connects to the given host and port.

           :param host: either host name (or IP) to connect to, or full URI
           :param port: port number of running MongoDB service on host
           :param username: An optional username for authentication.
           :param password: An optional password for authentication.
           :param database: The database to authenticate to if the URI
           specifies a username and password. If this is not specified but
           credentials exist, this defaults to the "admin" database. See
           mongoc_uri(7).
           :param p_file: SSL certificate and key file
           :param pem_pwd: Passphrase for encrypted key file
           :param ca_file: Certificate authority file
           :param ca_dir: Directory for certificate authority files
           :param c_file: Certificate revocation list file
           :param weak_cert_validation: bypass validation
           :param options: Connection-specific options as a dict.

           :returns: True if successful; false otherwise.
           :rtype: bool
        """
        if self._connection is not None:
            self.close()

        if host.startswith("mongodb://"):
            uri = host
        else:
            # Build up the URI string.
            uri = ["mongodb://"]
            if username is not None:
                if password is None:
                    uri.append("%s@" % username)
                else:
                    uri.append("%s:%s@" % (username, password))
            elif password is not None:
                raise ValueError("You cannot have a password with no"
                                 " username.")

            uri.append("%s:%d" % (host, port))

            if database is not None:
                uri.append("/%s" % database)
            if options is not None:
                uri.append("?%s" % urlencode(options))
            uri = "".join(uri)

        if sys.version >= "3":
            p_file = bytes(p_file, "ascii") if p_file is not None else None
            pem_pwd = bytes(pem_pwd, "ascii") if pem_pwd is not None else None
            ca_file = bytes(ca_file, "ascii") if ca_file is not None else None
            ca_dir = bytes(ca_dir, "ascii") if ca_dir is not None else None
            c_file = bytes(c_file, "ascii") if c_file is not None else None

        # Attempt the connection.
        err = get_empty_bson_error()
        self._connection = cmonary.monary_connect(
            uri.encode('ascii'),
            ctypes.c_char_p(p_file),
            ctypes.c_char_p(pem_pwd),
            ctypes.c_char_p(ca_file),
            ctypes.c_char_p(ca_dir),
            ctypes.c_char_p(c_file),
            ctypes.c_bool(weak_cert_validation),
            ctypes.byref(err))
        if self._connection is None:
            raise MonaryError(err.message)

    def _make_column_data(self, fields, types, count):
        """Builds the 'column data' structure used by the underlying cmonary
        code to populate the arrays.  This code must allocate the array
        objects, and provide their corresponding storage pointers and sizes
        to cmonary.

         :param fields: list of field names
         :param types: list of Monary type names
         :param count: size of storage to be allocated

         :returns: (coldata, colarrays) where coldata is the cmonary
                    column data storage structure, and colarrays is a list of
                    numpy.ndarray instances
         :rtype: tuple
        """

        err = get_empty_bson_error()

        numcols = len(fields)
        if numcols != len(types):
            raise ValueError("Number of fields and types do not match")
        if numcols > MAX_COLUMNS:
            raise ValueError("Number of fields exceeds maximum of %d"
                             % MAX_COLUMNS)
        coldata = cmonary.monary_alloc_column_data(numcols, count)
        if coldata is None:
            raise MonaryError("Unable to allocate column data")
        colarrays = []
        for i, (field, typename) in enumerate(zip(fields, types)):
            if len(field) > MAX_STRING_LENGTH:
                raise ValueError("Length of field name %s exceeds "
                                 "maximum of %d" % (field, MAX_COLUMNS))

            c_type, c_type_arg, numpy_type = get_monary_numpy_type(typename)

            data = numpy.zeros([count], dtype=numpy_type)
            mask = numpy.ones([count], dtype=bool)
            storage = numpy.ma.masked_array(data, mask)
            colarrays.append(storage)

            data_p = data.ctypes.data_as(ctypes.c_void_p)
            mask_p = mask.ctypes.data_as(ctypes.c_void_p)
            if cmonary.monary_set_column_item(
                    coldata,
                    i,
                    field.encode('ascii'),
                    c_type,
                    c_type_arg,
                    data_p,
                    mask_p,
                    ctypes.byref(err)) < 0:
                raise MonaryError(err.message)

        return coldata, colarrays

    def _get_collection(self, db, collection):
        """Returns the specified collection to query against.

            :param db: name of database
            :param collection: name of collection

            :returns: the collection
            :rtype: cmonary mongoc_collection_t*
        """
        if self._connection is not None:
            return cmonary.monary_use_collection(self._connection,
                                                 db.encode('ascii'),
                                                 collection.encode('ascii'))
        else:
            raise MonaryError("Unable to get the collection %s.%s - "
                              "not connected" % (db, collection))

    def count(self, db, coll, query=None):
        """Count the number of records returned by the given query.

           :param db: name of database
           :param coll: name of the collection to be queried
           :param query: (optional) dictionary of Mongo query parameters

           :returns: the number of records
           :rtype: int
        """
        collection = None
        err = get_empty_bson_error()
        try:
            collection = self._get_collection(db, coll)
            if collection is None:
                raise MonaryError("Unable to get the collection %s.%s" %
                                  (db, coll))
            query = make_bson(query)
            count = cmonary.monary_query_count(collection,
                                               query,
                                               ctypes.byref(err))
        finally:
            if collection is not None:
                cmonary.monary_destroy_collection(collection)
        if count < 0:
            raise MonaryError(err.message)
        return count

    def query(self, db, coll, query, fields, types,
              sort=None, hint=None,
              limit=0, offset=0,
              do_count=True, select_fields=False):
        """Performs an array query.

           :param db: name of database
           :param coll: name of the collection to be queried
           :param query: dictionary of Mongo query parameters
           :param fields: list of fields to be extracted from each record
           :param types: corresponding list of field types
           :param sort: (optional) single field name or list of
                        (field, direction) pairs
           :param hint: (optional) single field name or list of
                        (field, direction) pairs
           :param limit: (optional) limit number of records (and size
                         of arrays)
           :param offset: (optional) skip this many records before gathering
                          results
           :param bool do_count: count items before allocating arrays
                                 (otherwise, array size is set to limit)
           :param bool select_fields: select exact fields from database
                                      (performance/bandwidth tradeoff)

           :returns: list of numpy.ndarray, corresponding to the requested
                     fields and types
           :rtype: list
        """

        plain_query = get_plain_query(query)
        full_query = get_full_query(query, sort, hint)

        if not do_count and limit > 0:
            count = limit
        else:
            # count() doesn't like $query/$orderby/$hint, so need plain query.
            count = self.count(db, coll, plain_query)

        if count > limit > 0:
            count = limit

        coldata = None
        collection = None
        err = get_empty_bson_error()
        try:
            coldata, colarrays = self._make_column_data(fields, types, count)
            cursor = None
            try:
                collection = self._get_collection(db, coll)
                if collection is None:
                    raise MonaryError("Unable to get the collection")
                cursor = cmonary.monary_init_query(
                    collection,
                    offset,
                    limit,
                    full_query,
                    coldata,
                    select_fields,
                    ctypes.byref(err))
                if cursor is None:
                    raise MonaryError(err.message)
                if cmonary.monary_load_query(cursor, ctypes.byref(err)) < 0:
                    raise MonaryError(err.message)
            finally:
                if cursor is not None:
                    cmonary.monary_close_query(cursor)
                if collection is not None:
                    cmonary.monary_destroy_collection(collection)
        finally:
            if coldata is not None:
                cmonary.monary_free_column_data(coldata)
        return colarrays

    def block_query(self, db, coll, query, fields, types,
                    sort=None, hint=None,
                    block_size=8192, limit=0, offset=0,
                    select_fields=False):
        """Performs a block query.

           :param db: name of database
           :param coll: name of the collection to be queried
           :param query: dictionary of Mongo query parameters
           :param fields: list of fields to be extracted from each record
           :param types: corresponding list of field types
           :param sort: (optional) single field name or list of
                        (field, direction) pairs
           :param hint: (optional) single field name or list of
                        (field, direction) pairs
           :param block_size: (optional) size in number of rows of each
                              yeilded list
           :param limit: (optional) limit number of records (and size of
                         arrays)
           :param offset: (optional) skip this many records before gathering
                          results
           :param bool select_fields: select exact fields from database
                                      (performance/bandwidth tradeoff)

           :returns: list of numpy.ndarray, corresponding to the requested
                     fields and types
           :rtype: list

           A block query is a query whose results are returned in
           blocks of a given size.  Instead of returning a list of arrays,
           this generator yields portions of each array in multiple blocks,
           where each block may contain up to *block_size* elements.

           An example::

               cumulative_gain = 0.0
               for buy_price_block, sell_price_block in (
                    monary.block_query("finance", "assets", {"sold": True},
                                       ["buy_price", "sell_price"],
                                       ["float64", "float64"],
                                       block_size=1024)):
                    # Vector subtraction.
                    gain = sell_price_block - buy_price_block
                    cumulative_gain += numpy.sum(gain)

           .. note:: Memory for each block is reused between iterations.
                     If the caller wishes to retain the values from a given
                     iteration, it should copy the data.
        """

        if block_size < 1:
            block_size = 1

        full_query = get_full_query(query, sort, hint)

        coldata = None
        collection = None
        try:
            coldata, colarrays = self._make_column_data(fields,
                                                        types,
                                                        block_size)
            cursor = None
            try:
                collection = self._get_collection(db, coll)
                if collection is None:
                    raise MonaryError("Unable to get the collection")
                err = get_empty_bson_error()
                cursor = cmonary.monary_init_query(
                    collection,
                    offset,
                    limit,
                    full_query,
                    coldata,
                    select_fields,
                    ctypes.byref(err))
                if cursor is None:
                    raise MonaryError(err.message)
                while True:
                    num_rows = cmonary.monary_load_query(cursor,
                                                         ctypes.byref(err))
                    if num_rows < 0:
                        raise MonaryError(err.message)
                    if num_rows == block_size:
                        yield colarrays
                    elif num_rows > 0:
                        yield [arr[:num_rows] for arr in colarrays]
                        break
                    else:
                        break
            finally:
                if cursor is not None:
                    cmonary.monary_close_query(cursor)
                if collection is not None:
                    cmonary.monary_destroy_collection(collection)
        finally:
            if coldata is not None:
                cmonary.monary_free_column_data(coldata)

    def insert(self, db, coll, params, write_concern=None):
        """Performs an insertion of data from arrays.

           :param db: name of database
           :param coll: name of the collection to insert into
           :param params: list of MonaryParams to be inserted
           :param write_concern: (optional) a WriteConcern object.

           :returns: A numpy array of the inserted documents ObjectIds. Masked
                     values indicate documents that failed to be inserted.
           :rtype: numpy.ma.core.MaskedArray

           .. note:: Params will be sorted by field before insertion. To ensure
                     that _id is the first filled in all generated documents
                     and that nested keys are consecutive, all keys will be
                     sorted alphabetically before the insertions are performed.
                     The corresponding types and data will be sorted the same
                     way to maintain the original correspondence.
        """

        err = get_empty_bson_error()

        if len(params) == 0:
            raise ValueError("cannot do an empty insert")

        validate_insert_fields(list(map(lambda p: p.field, params)))

        # To ensure that _id is the first key, the string "_id" is mapped
        # to chr(0). This will put "_id" in front of any other field.
        params = sorted(
            params, key=lambda p: p.field if p.field != "_id" else chr(0))

        if params[0].field == "_id" and params[0].array.mask.any():
            raise ValueError("the _id array must not have any masked values")

        if len(set(len(p) for p in params)) != 1:
            raise ValueError("all given arrays must be of the same length")

        collection = None
        coldata = None
        id_data = None
        try:
            coldata = cmonary.monary_alloc_column_data(len(params),
                                                       len(params[0]))
            for i, param in enumerate(params):
                data_p = param.array.data.ctypes.data_as(ctypes.c_void_p)
                mask_p = param.array.mask.ctypes.data_as(ctypes.c_void_p)

                if cmonary.monary_set_column_item(
                        coldata,
                        i,
                        param.field.encode("utf-8"),
                        param.cmonary_type,
                        param.cmonary_type_arg,
                        data_p,
                        mask_p,
                        ctypes.byref(err)) < 0:
                    raise MonaryError(err.message)

            # Create a new column for the ids to be returned.
            id_data = cmonary.monary_alloc_column_data(1, len(params[0]))

            if params[0].field == "_id":
                # If the user specifies "_id", it will be sorted to the front.
                ids = numpy.copy(params[0].array)
                c_type = params[0].cmonary_type
                c_type_arg = params[0].cmonary_type_arg
            else:
                # Allocate a single column to return the generated ObjectIds.
                c_type, c_type_arg, numpy_type = get_monary_numpy_type("id")
                ids = numpy.zeros(len(params[0]), dtype=numpy_type)

            mask = numpy.ones(len(params[0]))
            ids = numpy.ma.masked_array(ids, mask)
            if cmonary.monary_set_column_item(
                    id_data,
                    0,
                    "_id".encode("utf-8"),
                    c_type,
                    c_type_arg,
                    ids.data.ctypes.data_as(ctypes.c_void_p),
                    ids.mask.ctypes.data_as(ctypes.c_void_p),
                    ctypes.byref(err)) < 0:
                raise MonaryError(err.message)

            collection = self._get_collection(db, coll)
            if collection is None:
                raise ValueError("unable to get the collection")

            if write_concern is None:
                write_concern = WriteConcern()

            cmonary.monary_insert(
                collection,
                coldata,
                id_data,
                self._connection,
                write_concern.get_c_write_concern(),
                ctypes.byref(err))

            return ids
        finally:
            if write_concern is not None:
                write_concern.destroy_c_write_concern()
            if coldata is not None:
                cmonary.monary_free_column_data(coldata)
            if id_data is not None:
                cmonary.monary_free_column_data(id_data)
            if collection is not None:
                cmonary.monary_destroy_collection(collection)

    def aggregate(self, db, coll, pipeline, fields, types, limit=0,
                  do_count=True):
        """Performs an aggregation operation.

           :param: db: name of database
           :param coll: name of collection on which to perform the aggregation
           :param pipeline: a list of pipeline stages
           :param fields: list of fields to be extracted from the result
           :param types: corresponding list of field types

           :returns: list of numpy.ndarray, corresponding to the requested
                     fields and types
           :rtype: list
        """
        # Convert the pipeline to a usable form.
        pipeline = get_pipeline(pipeline)

        # Determine sizing for array allocation.
        if not do_count and limit > 0:
            # Limit ourselves to only the first ``count`` records.
            count = limit
        else:
            # Use the aggregation pipeline to count the result size.
            count_stage = {"$group": {"_id": 1, "count": {"$sum": 1}}}
            pipe_copy = copy.deepcopy(pipeline)
            pipe_copy["pipeline"].append(count_stage)

            # Extract the count.
            result, = self.aggregate(db, coll, pipe_copy, ["count"], ["int64"],
                                     limit=1, do_count=False)
            result = result.compressed()
            if len(result) == 0:
                # The count returned was masked.
                raise MonaryError("Failed to count the aggregation size")
            else:
                count = result[0]

        if count > limit > 0:
            count = limit

        encoded_pipeline = get_plain_query(pipeline)
        coldata = None
        collection = None
        try:
            coldata, colarrays = self._make_column_data(fields, types, count)
            cursor = None
            try:
                collection = self._get_collection(db, coll)
                if collection is None:
                    raise MonaryError("Unable to get the collection")
                err = get_empty_bson_error()
                cursor = cmonary.monary_init_aggregate(collection,
                                                       encoded_pipeline,
                                                       coldata,
                                                       ctypes.byref(err))
                if cursor is None:
                    raise MonaryError(err.message)

                if cmonary.monary_load_query(cursor, ctypes.byref(err)) < 0:
                    raise MonaryError(err.message)
            finally:
                if cursor is not None:
                    cmonary.monary_close_query(cursor)
                if collection is not None:
                    cmonary.monary_destroy_collection(collection)
        finally:
            if coldata is not None:
                cmonary.monary_free_column_data(coldata)
        return colarrays

    def block_aggregate(self, db, coll, pipeline, fields, types,
                        block_size=8192, limit=0):
        """Performs an aggregation operation.

           Perform an aggregation operation on a collection, returning the
           results in blocks of size ``block_size``.
        """
        if block_size < 1:
            block_size = 1

        pipeline = get_pipeline(pipeline)
        encoded_pipeline = get_plain_query(pipeline)

        coldata = None
        collection = None
        try:
            coldata, colarrays = self._make_column_data(fields,
                                                        types,
                                                        block_size)
            cursor = None
            try:
                collection = self._get_collection(db, coll)
                if collection is None:
                    raise MonaryError("Unable to get the collection")
                err = get_empty_bson_error()
                cursor = cmonary.monary_init_aggregate(collection,
                                                       encoded_pipeline,
                                                       coldata,
                                                       ctypes.byref(err))
                if cursor is None:
                    raise MonaryError(err.message)

                err = get_empty_bson_error()
                while True:
                    num_rows = cmonary.monary_load_query(cursor,
                                                         ctypes.byref(err))
                    if num_rows < 0:
                        raise MonaryError(err.message)
                    if num_rows == block_size:
                        yield colarrays
                    elif num_rows > 0:
                        yield [arr[:num_rows] for arr in colarrays]
                        break
                    else:
                        break
            finally:
                if cursor is not None:
                    cmonary.monary_close_query(cursor)
                if collection is not None:
                    cmonary.monary_destroy_collection(collection)
        finally:
            if coldata is not None:
                cmonary.monary_free_column_data(coldata)

    def close(self):
        """Closes the current connection, if any."""
        if self._connection is not None:
            cmonary.monary_disconnect(self._connection)
            self._connection = None

    def __enter__(self):
        """Monary connections meet the ContextManager protocol."""
        return self

    def __exit__(self, *args):
        """Monary connections meet the ContextManager protocol."""
        self.close()

    def __del__(self):
        """Closes the Monary connection and cleans up resources."""
        self.close()
        self._cmonary = None
