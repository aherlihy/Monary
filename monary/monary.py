# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import atexit
import os.path
import platform
import sys
from ctypes import *

PY3 = sys.version_info[0] >= 3
if PY3:
    # Python 3
    bytes_type = bytes
    string_type = str
    from urllib.parse import urlencode
else:
    # Python 2.6/2.7
    bytes_type = basestring
    string_type = basestring
    from urllib import urlencode

try:
    # if we are using Python 2.7+
    from collections import OrderedDict
except ImportError:
    # for Python 2.6 and earlier
    from .ordereddict import OrderedDict

import numpy
import bson

cmonary = None

def _load_cmonary_lib():
    """Loads the cmonary CDLL library (from the directory containing this module)."""
    global cmonary
    thismodule = __file__
    abspath = os.path.abspath(thismodule)
    moduledir = list(os.path.split(abspath))[:-1]
    if platform.system() == 'Windows':
        libbson = CDLL(os.path.join(*(moduledir + ['libbson-1.0.dll'])))
        libcmongo = CDLL(os.path.join(*(moduledir + ['libmongoc-1.0.dll'])))
        cmonary_fname = "libcmonary.dll"
    else:
        cmonary_fname = "libcmonary.so"
    cmonaryfile = os.path.join(*(moduledir + [cmonary_fname]))
    cmonary = CDLL(cmonaryfile)

_load_cmonary_lib()

CTYPE_CODES = {
    "P": c_void_p,    # pointer
    "S": c_char_p,    # string
    "I": c_int,       # int
    "U": c_uint,      # unsigned int
    "L": c_long,      # long
    "0": None,        # None/void
}

# List of C function definitions from the cmonary library
FUNCDEFS = [
    # format: "func_name:arg_types:return_type"
    "monary_init::0",
    "monary_cleanup::0",
    "monary_connect:S:P",
    "monary_disconnect:P:0",
    "monary_use_collection:PSS:P",
    "monary_destroy_collection:P:0",
    "monary_alloc_column_data:UU:P",
    "monary_free_column_data:P:I",
    "monary_set_column_item:PUSUUPP:I",
    "monary_query_count:PP:L",
    "monary_init_query:PUUPPI:P",
    "monary_load_query:P:I",
    "monary_close_query:P:0",
]

MAX_COLUMNS = 1024
MAX_STRING_LENGTH = 1024

def _decorate_cmonary_functions():
    """Decorates each of the cmonary functions with their argument and result types."""
    for funcdef in FUNCDEFS:
        name, argtypes, restype = funcdef.split(":")
        func = getattr(cmonary, name)
        func.argtypes = [ CTYPE_CODES[c] for c in argtypes ]
        func.restype = CTYPE_CODES[restype]

_decorate_cmonary_functions()

# Initialize Monary and register the cleanup function
cmonary.monary_init()
atexit.register(cmonary.monary_cleanup)

# Table of type names and conversions between cmonary and numpy types
MONARY_TYPES = {
    # "common_name": (cmonary_type_code, numpy_type_object)
    "id":        (1, "<V12"),
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
    # The length argument here INCLUDES the null character
    "string":    (15, "S"),
    # Raw data (void pointer)
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
       ``int32``, ``float64``, ``date``, or ``string``.  If the type is ``string``,
       ``binary``, or ``bson``, its name must be followed by a ``:size`` suffix
       indicating the maximum number of bytes that will be used to store the
       representation.

       :param str orig_typename: a common type name with optional argument
                                 (for fields with a size)
       :returns: (type_num, type_arg, numpy_type)
       :rtype: tuple
    """
    # process any type_arg that might be included
    if ':' in orig_typename:
        vals = orig_typename.split(':', 2)
        if len(vals) > 2:
            raise ValueError("too many parts in type: %r" % orig_typename)
        type_name, arg = vals
        try:
            type_arg = int(arg)
        except ValueError:
            raise ValueError("unable to parse type argument in: %r" % orig_typename)
    else:
        type_arg = 0
        type_name = orig_typename

    if type_name not in MONARY_TYPES:
        raise ValueError("unknown typename: %r" % type_name)
    if type_name in ("string", "binary", "bson"):
        if type_arg == 0:
            raise ValueError("%r must have an explicit typearg with nonzero length "
                             "(use 'string:20', for example)" % type_name)
        type_num, numpy_type_code = MONARY_TYPES[type_name]
        numpy_type = "%s%i" % (numpy_type_code, type_arg)
    else:
        type_num, numpy_type = MONARY_TYPES[type_name]
    return type_num, type_arg, numpy_type

def make_bson(obj):
    """Given a Python (JSON compatible) dictionary, returns a BSON string.

       (This hijacks the Python -> BSON conversion code from pymongo, which is needed for
       converting queries.  Perhaps this dependency can be removed in a later version.)

       :param obj: object to be encoded as BSON (dict, string, or None)
       :returns: BSON encoded representation (byte string)
       :rtype: str
    """
    if obj is None:
        obj = { }
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
        # Python 3
        string = str(mvoid)
        string_list = ''.join(filter(lambda x: x not in '[]', string)).split()
        ints = map(int, string_list)
        uints = [x & 0xff for x in ints]
        id_bytes = bytes(uints)
        return bson.ObjectId(id_bytes)
    else:
        # Python 2.6 / 2.7
        return bson.ObjectId(str(mvoid))


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
        raise ValueError("invalid ordering: should be str or list of (column, direction) pairs")

def get_plain_query(query):
    """Composes a plain query from the given query object.
    
       :param dict query: query dictionary (or None)
       :returns: BSON encoded query (byte string)
       :rtype: str
    """
    if query is None:
        query = { }
    return make_bson(query)

def get_full_query(query, sort=None, hint=None):
    """Composes a full query from the given query object, and sort and hint clauses, if provided.
    
       :param dict query: query dictionary (or None)
       :param sort: (optional) single field name or list of (field, direction) pairs
       :param hint: (optional) single field name or list of (field, direction) pairs
       :returns: BSON encoded query (byte string)
       :rtype: str
    """
    if query is None:
        query = { }

    if sort or hint:
        query = OrderedDict([("$query", query)])
        if sort:
            try:
                query["$orderby"] = get_ordering_dict(sort)
            except ValueError:
                raise ValueError("sort arg must be string or list of (field, direction) pairs")
        if hint:
            try:
                query["$hint"] = get_ordering_dict(hint)
            except ValueError:
                raise ValueError("hint arg must be string or list of (field, direction) pairs")
    
    return make_bson(query)

class Monary(object):
    """Represents a 'monary' connection to a particular MongoDB server."""
    
    def __init__(self, host="localhost", port=27017, username=None,
                 password=None, database=None, options={}):
        """Initialize this connection with the given host and port.
        
           :param host: either host name (or IP) to connect to, or full URI
           :param port: port number of running MongoDB service on host
           :param username: An optional username for authentication.
           :param password: An optional password for authentication.
           :param database: The database to authenticate to if the URI
           specifies a username and password. If this is not specified but
           credentials exist, this defaults to the "admin" database. See
           mongoc_uri(7).
           :param options: Connection-specific options as a dict.
        """

        self._cmonary = cmonary
        self._connection = None
        if not self.connect(host, port, username, password, database, options):
            raise ValueError("Misformatted Mongo URI.")

    def connect(self, host="localhost", port=27017, username=None,
                password=None, database=None, options={}):
        """Connects to the given host and port.

           :param host: either host name (or IP) to connect to, or full URI
           :param port: port number of running MongoDB service on host
           :param username: An optional username for authentication.
           :param password: An optional password for authentication.
           :param database: The database to authenticate to if the URI
           specifies a username and password. If this is not specified but
           credentials exist, this defaults to the "admin" database. See
           mongoc_uri(7).
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
                raise ValueError("You cannot have a password with no username.")

            uri.append("%s:%d" % (host, port))

            if database is not None:
                uri.append("/%s" % database)
            if len(options) > 0:
                uri.append("?%s" % urlencode(options))
            uri = "".join(uri)

        # Attempt the connection
        self._connection = cmonary.monary_connect(uri.encode('ascii'))
        return (self._connection is not None)

    def _make_column_data(self, fields, types, count):
        """Builds the 'column data' structure used by the underlying cmonary code to
           populate the arrays.  This code must allocate the array objects, and provide
           their corresponding storage pointers and sizes to cmonary.

           :param fields: list of field names
           :param types: list of Monary type names
           :param count: size of storage to be allocated
           
           :returns: (coldata, colarrays) where coldata is the cmonary
                     column data storage structure, and colarrays is a list of
                     numpy.ndarray instances
           :rtype: tuple
        """

        if len(fields) != len(types):
            raise ValueError("number of fields and types do not match")
        numcols = len(fields)
        if numcols > MAX_COLUMNS:
            raise ValueError("number of fields exceeds maximum of %d" % MAX_COLUMNS)
        coldata = cmonary.monary_alloc_column_data(numcols, count)
        colarrays = [ ]
        for i, (field, typename) in enumerate(zip(fields, types)):
            if len(field) > MAX_STRING_LENGTH:
                raise ValueError("length of field name %s exceeds "
                                 "maximum of %d" % (field, MAX_COLUMNS))

            cmonary_type, cmonary_type_arg, numpy_type = get_monary_numpy_type(typename)

            data = numpy.zeros([count], dtype=numpy_type)
            mask = numpy.ones([count], dtype=bool)
            storage = numpy.ma.masked_array(data, mask)
            colarrays.append(storage)

            data_p = data.ctypes.data_as(c_void_p)
            mask_p = mask.ctypes.data_as(c_void_p)
            cmonary.monary_set_column_item(coldata, i, field.encode('ascii'),
                                           cmonary_type, cmonary_type_arg,
                                           data_p, mask_p)

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
            raise ValueError("failed to get collection %s.%s - not connected" % (db, collection))

    def count(self, db, coll, query=None):
        """Count the number of records that will be returned by the given query.
        
           :param db: name of database
           :param coll: name of the collection to be queried
           :param query: (optional) dictionary of Mongo query parameters
           
           :returns: the number of records
           :rtype: int
        """
        collection = None
        try:
            collection = self._get_collection(db, coll)
            if collection is None:
                raise ValueError("couldn't connect to collection %s.%s" % (db, coll))
            query = make_bson(query)
            count = cmonary.monary_query_count(collection, query)
        finally:
            if collection is not None:
                cmonary.monary_destroy_collection(collection)
        if count < 0:
            raise RuntimeError("Internal error in count()")
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
           :param sort: (optional) single field name or list of (field, direction) pairs
           :param hint: (optional) single field name or list of (field, direction) pairs
           :param limit: (optional) limit number of records (and size of arrays)
           :param offset: (optional) skip this many records before gathering results
           :param bool do_count: count items before allocating arrays
                                 (otherwise, array size is set to limit)
           :param bool select_fields: select exact fields from database
                                      (performance/bandwidth tradeoff)

           :returns: list of numpy.ndarray, corresponding to the requested fields and types
           :rtype: list
        """

        plain_query = get_plain_query(query)
        full_query = get_full_query(query, sort, hint)
        
        if not do_count and limit > 0:
            count = limit
        else:
            # count() doesn't like $query/$orderby/$hint clauses, so we need to use a plain query
            count = self.count(db, coll, plain_query)

        if count > limit > 0:
            count = limit

        coldata = None
        try:
            coldata, colarrays = self._make_column_data(fields, types, count)
            cursor = None
            try:
                collection = self._get_collection(db, coll)
                if collection is None:
                    raise ValueError("unable to get the collection")
                cursor = cmonary.monary_init_query(collection, offset, limit,
                                                   full_query, coldata, select_fields)
                cmonary.monary_load_query(cursor)
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
           :param sort: (optional) single field name or list of (field, direction) pairs
           :param hint: (optional) single field name or list of (field, direction) pairs
           :param block_size: (optional) size in number of rows of each yeilded list 
           :param limit: (optional) limit number of records (and size of arrays)
           :param offset: (optional) skip this many records before gathering results
           :param bool select_fields: select exact fields from database
                                      (performance/bandwidth tradeoff)

           :returns: list of numpy.ndarray, corresponding to the requested fields and types
           :rtype: list

           A block query is a query whose results are returned in
           blocks of a given size.  Instead of returning a list of arrays, this generator
           yields portions of each array in multiple blocks, where each block may contain
           up to *block_size* elements.

           An example::
        
               cumulative_gain = 0.0
               for buy_price_block, sell_price_block in (
                    monary.block_query("finance", "assets", {"sold": True},
                                       ["buy_price", "sell_price"],
                                       ["float64", "float64"],
                                       block_size=1024)):
                    gain = sell_price_block - buy_price_block   # vector subtraction
                    cumulative_gain += numpy.sum(gain)

           .. note:: Memory for each block is reused between iterations.  If the
                     caller wishes to retain the values from a given iteration, it
                     should copy the data.
        """

        if block_size < 1:
            block_size = 1

        full_query = get_full_query(query, sort, hint)

        coldata = None
        try:
            coldata, colarrays = self._make_column_data(fields, types, block_size)
            cursor = None
            try:
                collection = self._get_collection(db, coll)
                if collection is None:
                    raise ValueError("unable to get the collection")
                cursor = cmonary.monary_init_query(collection, offset, limit,
                                                   full_query, coldata, select_fields)
                while True:
                    num_rows = cmonary.monary_load_query(cursor)
                    if num_rows == block_size:
                        yield colarrays
                    elif num_rows > 0:
                        yield [ arr[:num_rows] for arr in colarrays ]
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
