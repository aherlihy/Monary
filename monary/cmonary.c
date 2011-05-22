// Monary - Copyright 2011 David J. C. Beach
// Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "mongo.h"
#include "bson.h"

#ifndef NDEBUG
#define DEBUG(format, ...) \
    fprintf(stderr, "[DEBUG] %s:%i " format "\n", __FILE__, __LINE__, ##__VA_ARGS__)
#else
#define DEBUG(...)
#endif

#define DEFAULT_MONGO_HOST "127.0.0.1"
#define DEFAULT_MONGO_PORT 27017

mongo_connection* monary_connect(const char* host,
                                 int port)
{
    if(host == NULL) { host = DEFAULT_MONGO_HOST; }
    if(port == 0) { port = DEFAULT_MONGO_PORT; }
    
    mongo_connection* conn = (mongo_connection*) malloc(sizeof(mongo_connection));
    
    DEBUG("attempting connection to: '%s' port %i", options.host, options.port);
    
    mongo_conn_return conn_result = mongo_connect(conn, host, port);
    
    if(conn_result == mongo_conn_success) {
        DEBUG("connected successfully");
        return conn;
    } else {
        DEBUG("received mongo_connect error code: %i\n", conn_result);
        return NULL;
    }
}

int monary_authenticate(mongo_connection* connection,
                        const char* db,
                        const char* user,
                        const char* pass)
{
    return mongo_cmd_authenticate(connection, db, user, pass);
}

void monary_disconnect(mongo_connection* connection)
{
    mongo_destroy(connection);
}

enum {
    TYPE_UNDEFINED = 0,
    TYPE_OBJECTID = 1,
    TYPE_BOOL = 2,
    TYPE_INT8 = 3,
    TYPE_INT16 = 4,
    TYPE_INT32 = 5,
    TYPE_INT64 = 6,
    TYPE_FLOAT32 = 7,
    TYPE_FLOAT64 = 8,
    TYPE_DATE = 9,       // BSON date (int64 storage)
    TYPE_TIMESTAMP = 10, // BSON timestamp (int64 storage)
    TYPE_STRING = 11,    // each record is (type_arg) chars in length
    TYPE_BINARY = 12,    // each record is (type_arg) bytes in length
    TYPE_TYPE = 13,      // BSON type code (int32 storage)
    TYPE_LENGTH = 14,    // length of string, symbol, binary, or bson object: int32 storage
    LAST_TYPE = 14,
};

typedef bson_oid_t OBJECTID;
typedef char BOOL;
typedef char INT8;
typedef short INT16;
typedef int INT32;
typedef int64_t INT64;
typedef float FLOAT32;
typedef double FLOAT64;

typedef struct monary_column_item monary_column_item;

struct monary_column_item
{
    char* field;
    unsigned int type;
    unsigned int type_arg;
    void* storage;
    unsigned char* mask;
};

typedef struct monary_column_data
{
    unsigned int num_columns;
    unsigned int num_rows;
    monary_column_item* columns;
} monary_column_data;

typedef struct monary_cursor {
    mongo_cursor* mcursor;
    monary_column_data* coldata;
} monary_cursor;

monary_column_data* monary_alloc_column_data(unsigned int num_columns,
                                             unsigned int num_rows)
{
    if(num_columns > 1024) { return NULL; }
    monary_column_data* result = (monary_column_data*) malloc(sizeof(monary_column_data));
    monary_column_item* columns = (monary_column_item*) calloc(num_columns, sizeof(monary_column_item));
    result->num_columns = num_columns;
    result->num_rows = num_rows;
    result->columns = columns;

    return result;
}

int monary_free_column_data(monary_column_data* coldata)
{
    if(coldata == NULL || coldata->columns == NULL) { return 0; }
    for(int i = 0; i < coldata->num_columns; i++) {
        monary_column_item* col = coldata->columns + i;
        if(col->field != NULL) { free(col->field); }
    }
    free(coldata->columns);
    free(coldata);
    return 1;
}

int monary_set_column_item(monary_column_data* coldata,
                           unsigned int colnum,
                           const char* field,
                           unsigned int type,
                           unsigned int type_arg,
                           void* storage,
                           unsigned char* mask)
{
    if(coldata == NULL) { return 0; }
    if(colnum >= coldata->num_columns) { return 0; }
    if(type == TYPE_UNDEFINED || type > LAST_TYPE) { return 0; }
    if(storage == NULL) { return 0; }
    if(mask == NULL) { return 0; }
    
    int len = strlen(field);
    if(len > 1024) { return 0; }
    
    monary_column_item* col = coldata->columns + colnum;

    col->field = (char*) malloc(len + 1);
    strcpy(col->field, field);
    
    col->type = type;
    col->type_arg = type_arg;
    col->storage = storage;
    col->mask = mask;

    return 1;
}

inline int monary_load_objectid_value(bson_iterator* bsonit,
                                      bson_type type,
                                      monary_column_item* citem,
                                      int idx)
{
    if(type == bson_oid) {
        OBJECTID* oid = bson_iterator_oid(bsonit);
        OBJECTID* oidloc = ((OBJECTID*) citem->storage) + idx;
        oidloc->ints[0] = oid->ints[0];
        oidloc->ints[1] = oid->ints[1];
        oidloc->ints[2] = oid->ints[2];
        return 1;
    } else {
        return 0;
    }
}

inline int monary_load_bool_value(bson_iterator* bsonit,
                                  bson_type type,
                                  monary_column_item* citem,
                                  int idx)
{
    BOOL value = bson_iterator_bool(bsonit);
    ((BOOL*) citem->storage)[idx] = value;
    return 1;
}

inline int monary_load_int8_value(bson_iterator* bsonit,
                                  bson_type type,
                                  monary_column_item* citem,
                                  int idx)
{
    if(type == bson_int || type == bson_long || type == bson_double) {
        INT8 value = bson_iterator_int(bsonit);
        ((INT8*)citem->storage)[idx] = value;
        return 1;
    } else {
        return 0;
    }
}

inline int monary_load_int16_value(bson_iterator* bsonit,
                                   bson_type type,
                                   monary_column_item* citem,
                                   int idx)
{
    if(type == bson_int || type == bson_long || type == bson_double) {
        INT16 value = bson_iterator_int(bsonit);
        ((INT16*)citem->storage)[idx] = value;
        return 1;
    } else {
        return 0;
    }
}

inline int monary_load_int32_value(bson_iterator* bsonit,
                                   bson_type type,
                                   monary_column_item* citem,
                                   int idx)
{
    if(type == bson_int || type == bson_long || type == bson_double) {
        INT32 value = bson_iterator_int(bsonit);
        ((INT32*)citem->storage)[idx] = value;
        return 1;
    } else {
        return 0;
    }
}

inline int monary_load_int64_value(bson_iterator* bsonit,
                                   bson_type type,
                                   monary_column_item* citem,
                                   int idx)
{
    if(type == bson_int || type == bson_long || type == bson_double) {
        INT64 value = bson_iterator_int(bsonit);
        ((INT64*)citem->storage)[idx] = value;
        return 1;
    } else {
        return 0;
    }
}

inline int monary_load_float32_value(bson_iterator* bsonit,
                                     bson_type type,
                                     monary_column_item* citem,
                                     int idx)
{
    if(type == bson_int || type == bson_long || type == bson_double) {
        FLOAT32 value = bson_iterator_double(bsonit);
        ((FLOAT32*) citem->storage)[idx] = value;
        return 1;
    } else {
        return 0;
    }
}

inline int monary_load_float64_value(bson_iterator* bsonit,
                                     bson_type type,
                                     monary_column_item* citem,
                                     int idx)
{
    if(type == bson_int || type == bson_long || type == bson_double) {
        FLOAT64 value = bson_iterator_double(bsonit);
        ((FLOAT64*) citem->storage)[idx] = value;
        return 1;
    } else {
        return 0;
    }
}

inline int monary_load_date_value(bson_iterator* bsonit,
                                  bson_type type,
                                  monary_column_item* citem,
                                  int idx)
{
    if(type == bson_date) {
        bson_date_t value = bson_iterator_date(bsonit);
        ((INT64*) citem->storage)[idx] = value;
        return 1;
    } else {
        return 0;
    }
}

inline int monary_load_timestamp_value(bson_iterator* bsonit,
                                       bson_type type,
                                       monary_column_item* citem,
                                       int idx)
{
    if(type == bson_timestamp) {
        bson_timestamp_t value = bson_iterator_timestamp(bsonit);
        ((INT64*) citem->storage)[idx] = *((INT64*) &value);
        return 1;
    } else {
        return 0;
    }
}

inline int monary_load_string_value(bson_iterator* bsonit,
                                    bson_type type,
                                    monary_column_item* citem,
                                    int idx)
{
    if(type == bson_string || type == bson_symbol || type == bson_code) {
        size_t size = citem->type_arg;
        const char* src = bson_iterator_string(bsonit);
        char* dest = ((char*) citem->storage) + (idx * size);
        strncpy(dest, src, size);
        return 1;
    } else {
        return 0;
    }
}

inline int monary_load_binary_value(bson_iterator* bsonit,
                                    bson_type type,
                                    monary_column_item* citem,
                                    int idx)
{
    if(type == bson_bindata) {
        int size = citem->type_arg;
        int binlen = bson_iterator_bin_len(bsonit);
        if(binlen > size) { binlen = size; }
        const char* src = bson_iterator_bin_data(bsonit);
        char* dest = ((char*) citem->storage) + (idx * size);
        memcpy(dest, src, binlen);
        return 1;
    } else {
        return 0;
    }
}

inline int monary_load_type_value(bson_iterator* bsonit,
                                  bson_type type,
                                  monary_column_item* citem,
                                  int idx)
{
    ((INT32*) citem->storage)[idx] = type;
    return 1;
}

inline int monary_load_length_value(bson_iterator* bsonit,
                                    bson_type type,
                                    monary_column_item* citem,
                                    int idx)
{
    int length = -1;
    if(type == bson_string || type == bson_code || type == bson_symbol) {
        // Note: Python's len() counts the characters in a string.  However,
        // c-mongo's bson_iterator_string_len fuction includes a NUL terminator
        // in the count.  Here, we subtract 1 to make it agree with Python.
        length = bson_iterator_string_len(bsonit) - 1;
    } else if(type == bson_bindata) {
        length = bson_iterator_bin_len(bsonit);
    } else if(type == bson_array || type == bson_object) {
        bson subobj;
        bson_iterator_subobject(bsonit, &subobj);
        length = bson_size(&subobj);
    }
    ((INT32*) citem->storage)[idx] = length;
    if(length >= 0) {
        return 1;
    } else {
        return 0;
    }
}

int monary_load_item(bson_iterator* bsonit,
                     bson_type type,
                     monary_column_item* citem,
                     int offset)
{
    int success = 0;
    switch(citem->type) {
        case TYPE_OBJECTID:
            success = monary_load_objectid_value(bsonit, type, citem, offset);
            break;
        case TYPE_BOOL:
            success = monary_load_bool_value(bsonit, type, citem, offset);
            break;
        case TYPE_INT8:
            success = monary_load_int8_value(bsonit, type, citem, offset);
            break;
        case TYPE_INT16:
            success = monary_load_int16_value(bsonit, type, citem, offset);
            break;
        case TYPE_INT32:
            success = monary_load_int32_value(bsonit, type, citem, offset);
            break;
        case TYPE_INT64:
            success = monary_load_int64_value(bsonit, type, citem, offset);
            break;
        case TYPE_FLOAT32:
            success = monary_load_float32_value(bsonit, type, citem, offset);
            break;
        case TYPE_FLOAT64:
            success = monary_load_float64_value(bsonit, type, citem, offset);
            break;
        case TYPE_DATE:
            success = monary_load_date_value(bsonit, type, citem, offset);
            break;
        case TYPE_TIMESTAMP:
            success = monary_load_timestamp_value(bsonit, type, citem, offset);
            break;
        case TYPE_STRING:
            success = monary_load_string_value(bsonit, type, citem, offset);
            break;
        case TYPE_BINARY:
            success = monary_load_binary_value(bsonit, type, citem, offset);
            break;
        case TYPE_TYPE:
            success = monary_load_type_value(bsonit, type, citem, offset);
            break;
        case TYPE_LENGTH:
            success = monary_load_length_value(bsonit, type, citem, offset);
            break;
        default:
            success = 0;
            break;
    }
    return success;
}

int monary_bson_to_arrays(monary_column_data* coldata,
                          unsigned int row,
                          bson* bson_data)
{
    int num_masked = 0;

    bson_iterator bsonit;
    for(int i = 0; i < coldata->num_columns; i++) {
        monary_column_item* citem = coldata->columns + i;
        bson_type found_type = bson_find(&bsonit, bson_data, citem->field);
        int success = 0;

        int offset = row;

        // dispatch to appropriate column handler (inlined)
        if(found_type) {
            success = monary_load_item(&bsonit, found_type, citem, offset);
        }

        // record success result in mask, if applicable
        if(citem->mask != NULL) {
            citem->mask[offset] = !success;
        }

        // tally number of masked (unsuccessful) loads
        if(!success) { ++num_masked; }
    }
    
    return num_masked;
}

long monary_query_count(mongo_connection* connection,
                        const char* db_name,
                        const char* coll_name,
                        const char* query)
{

    // build BSON query data
    bson query_bson;
    bson_init(&query_bson, (char*) query, 0);
    
    long total_count = mongo_count(connection, db_name, coll_name, &query_bson);
    
    bson_destroy(&query_bson);

    return total_count;
}

void monary_get_bson_fields_list(monary_column_data* coldata,
                                 bson* fields_bson)
{
    bson_buffer fields_builder;
    bson_buffer_init(&fields_builder);
    for(int i = 0; i < coldata->num_columns; i++) {
        monary_column_item* col = coldata->columns + i;
        bson_append_int(&fields_builder, col->field, 1);
    }
    bson_from_buffer(fields_bson, &fields_builder);
}

monary_cursor* monary_init_query(mongo_connection* connection,
                                 const char* ns,
                                 const char* query,
                                 int limit,
                                 int offset,
                                 monary_column_data* coldata,
                                 int select_fields)
{
    // build BSON query data
    bson query_bson;
    bson_init(&query_bson, (char*) query, 0);

    // build BSON fields list (if necessary)
    bson query_fields;
    if(select_fields) { monary_get_bson_fields_list(coldata, &query_fields); }

    // create query cursor
    bson* fields_ptr = select_fields ? &query_fields : NULL;
    mongo_cursor* mcursor = mongo_find(connection, ns, &query_bson, fields_ptr, limit, offset, 0);

    // destroy BSON fields
    bson_destroy(&query_bson);
    if(select_fields) { bson_destroy(&query_fields); }

    monary_cursor* cursor = (monary_cursor*) malloc(sizeof(monary_cursor));
    cursor->mcursor = mcursor;
    cursor->coldata = coldata;
    
    return cursor;
}

int monary_load_query(monary_cursor* cursor)
{
    mongo_cursor* mcursor = cursor->mcursor;
    monary_column_data* coldata = cursor->coldata;
    
    // read result values
    int row = 0;
    int num_masked = 0;
    while(mongo_cursor_next(mcursor) && row < coldata->num_rows) {

#ifndef NDEBUG
        if(row % 500000 == 0) {
            DEBUG("...%i rows loaded", row);
        }
#endif

        num_masked += monary_bson_to_arrays(coldata, row, &(mcursor->current));
        ++row;
    }

    int total_values = row * coldata->num_columns;
    DEBUG("%i rows loaded; %i / %i values were masked", row, num_masked, total_values);

    return row;
}

void monary_close_query(monary_cursor* cursor)
{
    mongo_cursor_destroy(cursor->mcursor);
    free(cursor);
}
