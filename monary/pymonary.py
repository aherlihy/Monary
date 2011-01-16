#!/usr/bin/env python
#######################################################################
#
#     Monary
#
#     Convert Mongo queries into Numpy column arrays
#
#     Copyright 2005-2010  David J. C. Beach
#     DJC Innovations LLC  http://www.djcinnovations.com
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
#
#######################################################################

import operator

import pymongo
import numpy as np

OBJECTID_DTYPE = "|S12"

def column_arrays(query, fields, count=None, log=False):
    """Returns column arrays corresponding to the requested fields
       from documents in the given PyMongo query object.  Fields
       is a list of strings, written as "fieldname:typecode".
       Typecode should be a valid Numpy dtype, and must be present
       in every field.  The special typecode "id" may be used to
       extract and store ObjectIDs.

       If provided, 'count' is be the number of objects returned
       by the query.  If not provided, query.count() will be called
       to determine the appropriate value.

       This function returns a numpy.ma.masked_array() instance for
       each requested column.  If a field is not present in a record,
       or if its value is not compatible with the array type, then
       the corresponding element will be masked in the result.

       Example:
       
       column_arrays(collection.find(),
           ["_id:id", "name:10unicode", "age:int", "weight:float"])
       (Returns id, name, age, and weight columns.)
    """

    if count is None:
        count = query.count()

    getters_columns = [ _setup_field(field, count) for field in fields ]

    for i, record in enumerate(query):
        if log and i % 100000 == 0:
            print "... processing record %i / %i" % (i, count)
        for getter, column in getters_columns:
            try:
                column[i] = getter(record)
            except (KeyError, ValueError):
                column.mask[i] = True

    return [ column for getter, column in getters_columns ]

def _setup_field(field, count):
    fname, dtype = field.split(':', 1)

    if dtype == "id":
        getter = lambda record: record[fname].binary
        dtype = OBJECTID_DTYPE
    else:
        getter = operator.itemgetter(fname)

    array = np.zeros([count], dtype=dtype)
    mask = np.zeros([count], dtype=bool)
    storage = np.ma.masked_array(array, mask)
    
    return getter, storage

def insert_records(coll, valdict):
    fields_values = list(valdict.items())
    size = len(valdict.values()[0])
    if "_id" not in valdict:
        idcol = np.ma.masked_array(np.empty([count], dtype=OBJECTID_DTYPE), False)
        set_ids = True
    else:
        idcol = valdict["_id"]
    for i in xrange(size):
        record = { field: values[i] for field, values in fields_values }
        coll.insert(record)
        if set_ids:
            idcol[i] = record['_id']
    return idcol

def update_records(coll, wheredict, valdict):
    size = len(wheredict.values()[0])
    where_values = list(wheredict.items())
    set_values = list(valdict.items())
    for i in xrange(size):
        where = { field: values[i] for field, values in where_values }
        changes = { field: values[i] for field, values in set_values }
        coll.update(where, changes)
