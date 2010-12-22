from ctypes import *
import numpy
from pymongo.helpers import bson

cmonary = CDLL("libcmonary.dylib")

FUNCDEFS = [
    ("monary_connect", [c_char_p, c_int], c_void_p),
    ("monary_disconnect", [c_void_p], None),
    ("monary_alloc_column_data", [c_uint, c_uint], c_void_p),
    ("monary_free_column_data", [c_void_p], c_int),
    ("monary_set_column_item", [c_void_p, c_uint, c_char_p, c_uint, c_uint, c_void_p, c_void_p], c_int),
    ("monary_query_count", [c_void_p, c_char_p, c_char_p, c_char_p], c_long),
    ("monary_init_query", [c_void_p, c_char_p, c_char_p, c_int, c_int, c_void_p, c_int], c_void_p),
    ("monary_load_query", [c_void_p], c_int),
    ("monary_close_query", [c_void_p], None),
]

for name, argtypes, restype in FUNCDEFS:
    func = getattr(cmonary, name)
    func.argtypes = argtypes
    func.restype = restype

MONARY_TYPES = {
    "id": (1, "|S12"),
    "int32": (2, numpy.int32),
    "float64": (3, numpy.float64),
    "bool": (4, numpy.bool),
}

def make_bson(obj):
    if obj is None:
        obj = { }
    if not isinstance(obj, basestring):
        obj = bson.BSON.encode(obj)
    return obj

class Monary(object):
    
    def __init__(self, host=None, port=0):
        self._connection = None
        self.connect(host, port)
            
    def connect(self, host=None, port=0):
        if self._connection is not None:
            self.close()
        self._connection = cmonary.monary_connect(host, port)

    def _make_column_data(self, fields, types, count):
        numcols = len(fields)
        coldata = cmonary.monary_alloc_column_data(numcols, count)
        colarrays = [ ]
        for i, (field, typename) in enumerate(zip(fields, types)):
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
            cmonary.monary_set_column_item(coldata, i, field, cmonary_type, 0, data_p, mask_p)

        return coldata, colarrays

    def count(self, db, coll, query=None):
        query = make_bson(query)
        return cmonary.monary_query_count(self._connection, db, coll, query)

    def query(self, db, coll, query, fields, types, limit=0, offset=0,
              do_count=True, select_fields=False):
        query = make_bson(query)
        if not do_count and limit > 0:
            count = limit
        else:
            count = self.count(db, coll, query)
        if count > limit > 0: count = limit
        print "allocating arrays of size %i" % count

        coldata = None
        try:
            coldata, colarrays = self._make_column_data(fields, types, count)
            ns = "%s.%s" % (db, coll)
            cursor = None
            try:
                cursor = cmonary.monary_init_query(self._connection, ns, query, limit, offset,
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
        if self._connection is not None:
            cmonary.monary_disconnect(self._connection)
            self._connection = None
        
    def __del__(self):
        self.close()

monary = Monary("127.0.0.1")
arrays =  monary.query("test", "spam", {"float5": {"$gt": 6.8}}, ["_id", "intval", "float1", "float2", "float3", "float4", "float5", "float6", "float7", "float8", "float9", "float10"], ["id", "int32", "float64", "float64", "float64", "float64", "float64", "float64", "float64", "float64", "float64", "float64"], limit=5000000, do_count=False)
monary.close()

#import struct
# (b1, b2, b3) = struct.unpack(">III", arrays[0][0])
# val = (b1 << 64) + (b2 << 32) + b3
# print "%x" % val

for array in arrays[1:]:
    #print array[0:10]
    print numpy.mean(array)


