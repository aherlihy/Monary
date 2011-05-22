# Monary - Copyright 2011 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import os.path
from ctypes import *
from collections import OrderedDict

import numpy
import bson

cmonary = None

def _load_cmonary_lib():
    """Loads the cmonary CDLL library (from the directory containing this module)."""
    global cmonary
    thismodule = __file__
    abspath = os.path.abspath(thismodule)
    moduledir = list(os.path.split(abspath))[:-1]
    cmonaryfile = os.path.join(*(moduledir + ["libcmonary.so"]))
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
    "monary_connect:SI:P",
    "monary_authenticate:PSSS:I",
    "monary_disconnect:P:0",
    "monary_alloc_column_data:UU:P",
    "monary_free_column_data:P:I",
    "monary_set_column_item:PUSUUPPUII:I",
    "monary_query_count:PSSS:L",
    "monary_init_query:PSSIIPI:P",
    "monary_load_query:P:I",
    "monary_close_query:P:0",
]

def _decorate_cmonary_functions():
    """Decorates each of the cmonary functions with their argument and result types."""
    for funcdef in FUNCDEFS:
        name, argtypes, restype = funcdef.split(":")
        func = getattr(cmonary, name)
        func.argtypes = [ CTYPE_CODES[c] for c in argtypes ]
        func.restype = CTYPE_CODES[restype]

_decorate_cmonary_functions()

# Table of type names and conversions between cmonary and numpy types
MONARY_TYPES = {
    # "common_name": (cmonary_type_code, numpy_type_object)
    "id":        (1, "|V12"),
    "bool":      (2, numpy.bool),
    "int8":      (3, numpy.int8),
    "int16":     (4, numpy.int16),
    "int32":     (5, numpy.int32),
    "int64":     (6, numpy.int64),
    "float32":   (7, numpy.float32),
    "float64":   (8, numpy.float64),
    "date":      (9, numpy.int64),
    "timestamp": (10, numpy.int64),
    "type":      (11, numpy.int32),
    "length":    (12, numpy.int32),
}

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
    if not isinstance(obj, basestring):
        obj = bson.BSON.encode(obj)
    return obj

def get_ordering_dict(obj):
    """Converts a field/direction specification to an OrderedDict, suitable
       for BSON encoding.
    
       :param obj: single field name or list of (field, direction) pairs
       :returns: mapping representing the field/direction list
       :rtype: OrderedDict
    """
    if obj is None:
        return OrderedDict()
    elif isinstance(obj, basestring):
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
                query["$sort"] = get_ordering_dict(sort)
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
    
    def __init__(self, host=None, port=0):
        """Initialize this connection with the given host and port.
        
           :param host: host name (or IP) to connect
           :param port: port number of running MongoDB service on host
        """
        self._cmonary = cmonary
        self._connection = None
        self.connect(host, port)
            
    def connect(self, host=None, port=0):
        """Connects to the given host and port.

           :param host: host name (or IP) to connect
           :param port: port number of running MongoDB service on host

           :returns: True if connection was successful, False otherwise.
           :rtype: bool
        """
        if self._connection is not None:
            self.close()
        self._connection = cmonary.monary_connect(host, port)
        success = (self._connection is not None)
        return success

    def authenticate(self, db, user, passwd):
        """Authenticate this Monary connection for a given database with the provided
           username and password.
        
            :param db: name of database
            :param user: name of authenticating user
            :param passwd: password for user
            
            :returns: True if authentication was successful, False otherwise.
            :rtype: bool
        """
        
        assert self._connection is not None, "Not connected"
        result = cmonary.monary_authenticate(self._connection, db, user, passwd)
        return bool(result)

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
        coldata = cmonary.monary_alloc_column_data(numcols, count)
        colarrays = [ ]
        for i, (field, typename) in enumerate(zip(fields, types)):
            if typename not in MONARY_TYPES:
                raise ValueError("not a valid monary type name: %s" % typename)
            
            cmonary_type, numpy_type = MONARY_TYPES[typename]

            # BUG: how do we default to masking all values in the array
            # I seem to be having some trouble setting this up
            # (it's a real issue if the array is oversized (allocated too large),
            # and therefore isn't filled with data...)

            data = numpy.zeros([count], dtype=numpy_type)
            mask = numpy.ones([count], dtype=bool)
            storage = numpy.ma.masked_array(data, mask)
            colarrays.append(storage)

            data_p = data.ctypes.data_as(c_void_p)
            mask_p = mask.ctypes.data_as(c_void_p)
            cmonary.monary_set_column_item(coldata, i, field, cmonary_type, 0, data_p, mask_p,
                                           1, 1, 0)

        return coldata, colarrays

    def count(self, db, coll, query=None):
        """Count the number of records that will be returned by the given query.
        
           :param db: name of database
           :param coll: name of the collection to be queried
           :param query: (optional) dictionary of Mongo query parameters
           
           :returns: the number of records
           :rtype: int
        """
        
        query = make_bson(query)
        return cmonary.monary_query_count(self._connection, db, coll, query)

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
           :param bool select_fields: select exact fields from database (performance/bandwidth tradeoff)

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
            ns = "%s.%s" % (db, coll)
            cursor = None
            try:
                cursor = cmonary.monary_init_query(self._connection, ns, full_query, limit, offset,
                                                   coldata, select_fields)
                cmonary.monary_load_query(cursor)
            finally:
                if cursor is not None:
                    cmonary.monary_close_query(cursor)
        finally:
            if coldata is not None:
                cmonary.monary_free_column_data(coldata)
        return colarrays

    def close(self):
        """Closes the current connection, if any."""
        if self._connection is not None:
            cmonary.monary_disconnect(self._connection)
            self._connection = None
        
    def __enter__(self):
        """Monary connections meet the ContextManager protocol."""
        return self
        
    def __exit__(self, *args, **kw):
        """Monary connections meet the ContextManager protocol."""
        self.close()
        
    def __del__(self):
        """Closes the Monary connection and cleans up resources."""
        self.close()
        self._cmonary = None
