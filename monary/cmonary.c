// Monary - Copyright 2011-2014 David J. C. Beach
// Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "mongoc.h"
#include "bson.h"

#ifndef NDEBUG
#define DEBUG(format, ...) fprintf(stderr, "[DEBUG] %s:%i " format "\n", __FILE__, __LINE__, __VA_ARGS__)
#else
#define DEBUG(...)
#endif

#define MONARY_MAX_NUM_COLUMNS 1024
#define MONARY_MAX_STRING_LENGTH 1024
#define MONARY_MAX_QUERY_LENGTH 4096
#define MONARY_MAX_RECURSION 100

enum {
    TYPE_UNDEFINED = 0,
    TYPE_OBJECTID = 1,
    TYPE_BOOL = 2,
    TYPE_INT8 = 3,
    TYPE_INT16 = 4,
    TYPE_INT32 = 5,
    TYPE_INT64 = 6,
    TYPE_UINT8 = 7,
    TYPE_UINT16 = 8,
    TYPE_UINT32 = 9,
    TYPE_UINT64 = 10,
    TYPE_FLOAT32 = 11,
    TYPE_FLOAT64 = 12,
    TYPE_DATE = 13,     // BSON date-time, seconds since the UNIX epoch (uint64 storage)
    TYPE_TIMESTAMP = 14,        // BSON timestamp - UNIX timestamp and increment (uint64 storage)
    TYPE_STRING = 15,   // each record is (type_arg) chars in length
    TYPE_BINARY = 16,   // each record is (type_arg) bytes in length
    TYPE_BSON = 17,     // BSON subdocument as binary (each record is type_arg bytes)
    TYPE_TYPE = 18,     // BSON type code (uint8 storage)
    TYPE_SIZE = 19,     // data size of a string, symbol, binary, or bson object (uint32)
    TYPE_LENGTH = 20,   // length of string (character count) or num elements in BSON (uint32)
    LAST_TYPE = 20      // BSON type code as per the BSON specification
};


void
initlibcmonary(void)
{}

/**
 * Helper function to set the fields of bson_error_t
 */
void
monary_error(bson_error_t * err, char *message)
{
    err->code = 0;
    err->domain = 0;
    bson_snprintf(err->message, sizeof(err->message), "%s", message);
}

/**
 * Controls the logging performed by libmongoc.
 */
void
monary_log_func(mongoc_log_level_t log_level,
                const char *log_domain, const char *message, void *user_data)
{
    return;
}

/**
 * Initialize libmongoc.
 */
void
monary_init(void)
{
    mongoc_init();
#ifdef NDEBUG
    mongoc_log_set_handler(monary_log_func, NULL);
#endif
    DEBUG("%s", "monary module initialized");
}

/**
 * Releases resources used by libmongoc.
 */
void
monary_cleanup(void)
{
    mongoc_cleanup();
    DEBUG("%s", "monary module cleaned up");
}

/**
 * Makes a new connection to a MongoDB server and database.
 *
 * @param uri A MongoDB URI, as per mongoc_uri(7).
 * @param err bson_error_t that holds error information in case of failure
 *
 * @return A pointer to a mongoc_client_t, or NULL if the connection attempt
 * was unsuccessful.
 */
mongoc_client_t *
monary_connect(const char *uri, const char *pem_file,
               const char *pem_pwd, const char *ca_file,
               const char *ca_dir, const char *crl_file,
               bool weak_cert_validation, bson_error_t * err)
{

    mongoc_client_t *client;
    mongoc_uri_t *mongo_uri;

    if (!uri) {
        monary_error(err, "empty URI passed to monary_connect");
        return NULL;
    }

    DEBUG("Attempting connection to: %s", uri);
    client = mongoc_client_new(uri);
    if (!client) {
        monary_error(err, "cmongo failed to parse URI in monary_connect");
        return NULL;
    }
    DEBUG("%s", "Connection successful");
    mongo_uri = mongoc_uri_new(uri);

    if (mongoc_uri_get_ssl(mongo_uri)) {
        mongoc_ssl_opt_t opts = { pem_file, pem_pwd, ca_file, ca_dir, crl_file,
            weak_cert_validation
        };
        mongoc_client_set_ssl_opts(client, &opts);
        DEBUG("Setting SSL opts={%s, %s, %s, %s, %s, %i} \n",
              pem_file, pem_pwd, ca_file, ca_dir, crl_file,
              weak_cert_validation);
    }
    mongoc_uri_destroy(mongo_uri);
    return client;
}

/**
 * Destroys all resources associated with the client.
 */
void
monary_disconnect(mongoc_client_t * client)
{
    DEBUG("%s", "Closing mongoc_client");
    mongoc_client_destroy(client);
}

/**
 * Use a particular database and collection from the given MongoDB client.
 *
 * @param client A mongoc_client_t that has been properly connected to with
 * mongoc_client_new().
 * @param db A valid ASCII C string for the name of the database.
 * @param collection A valid ASCII C string for the collection name.
 *
 * @return If successful, a mongoc_collection_t that can be queried against
 * with mongoc_collection_find(3).
 */
mongoc_collection_t *
monary_use_collection(mongoc_client_t * client,
                      const char *db, const char *collection)
{
    return mongoc_client_get_collection(client, db, collection);
}

/**
 * Destroys the given collection, allowing you to connect to another one.
 *
 * @param collection The collection to destroy.
 */
void
monary_destroy_collection(mongoc_collection_t * collection)
{
    if (collection) {
        DEBUG("%s", "Closing mongoc_collection");
        mongoc_collection_destroy(collection);
    }
}

/**
 * Holds the storage for an array of objects.
 *
 * @memb field The name of the field in the document.
 * @memb type The BSON type identifier, as specified by the Monary type enum.
 * @memb type_arg If type is binary, UTF-8, or document, type_arg specifies the
 * width of the field in bytes.
 * @memb storage A pointer to the location of the "array" in memory. In the
 * Python side of Monary, this points to the start of the NumPy array.
 * @memb mask A pointer to the the "masked array." This is the internal
 * representation of the NumPy ma.array, which corresponds one-to-one to the
 * storage array. A value is masked if and only if an error occurs while
 * loading memory from MongoDB.
 */
typedef struct monary_column_item {
    char *field;
    unsigned int type;
    unsigned int type_arg;
    void *storage;
    unsigned char *mask;
} monary_column_item;

/**
 * Represents a collection of arrays.
 *
 * @memb num_columns The number of arrays to track, one per field.
 * @memb num_rows The number of elements per array. (Specifically, each
 * monary_column_item.storage contains num_rows elements.)
 * @memb columns A pointer to the first array.
 */
typedef struct monary_column_data {
    unsigned int num_columns;
    unsigned int num_rows;
    monary_column_item *columns;
} monary_column_data;

/**
 * A MongoDB cursor augmented with Monary column data.
 */
typedef struct monary_cursor {
    mongoc_cursor_t *mcursor;
    monary_column_data *coldata;
} monary_cursor;

/**
 * Allocates heap space for data storage.
 *
 * @param num_columns The number of fields to store (that is, the number of
 * internal monary_column_item structs tracked by the column data structure).
 * Cannot exceed MONARY_MAX_NUM_COLS.
 * @param num_rows The lengths of the arrays managed by each column item.
 *
 * @return A pointer to the newly-allocated column data.
 */
monary_column_data *
monary_alloc_column_data(unsigned int num_columns, unsigned int num_rows)
{
    monary_column_data *result;

    monary_column_item *columns;

    if (num_columns > MONARY_MAX_NUM_COLUMNS) {
        return NULL;
    }
    result = (monary_column_data *) malloc(sizeof(monary_column_data));
    columns =
        (monary_column_item *) calloc(num_columns, sizeof(monary_column_item));

    DEBUG("%s", "Column data allocated");

    result->num_columns = num_columns;
    result->num_rows = num_rows;
    result->columns = columns;

    return result;
}

int
monary_free_column_data(monary_column_data * coldata)
{
    int i;

    monary_column_item *col;

    if (coldata == NULL || coldata->columns == NULL) {
        return 0;
    }

    for (i = 0; i < coldata->num_columns; i++) {
        col = coldata->columns + i;
        if (col->field != NULL) {
            free(col->field);
        }
    }
    free(coldata->columns);
    free(coldata);
    return 1;
}

/**
 * Sets the field value for a particular column and item. This is referenced by
 * Monary._make_column_data.
 *
 * @param coldata A pointer to the column data to modify.
 * @param colnum The number of the column item within the table to modify
 * (representing one data field). Columns are indexed starting from zero.
 * @param field The new name of the column item. Cannot exceed
 * MONARY_MAX_STRING_LENGTH characters in length.
 * @param type The new type of the item.
 * @param type_arg For UTF-8, binary and BSON types, specifies the size of the
 * data.
 * @param storage A pointer to the new location of the data in memory, which
 * cannot be NULL. Note that this does not free(3) the previous storage
 * pointer.
 * @param mask A pointer to the new masked array, which cannot be NULL. It is a
 * programming error to have a masked array different in length from the
 * storage array.
 * @param err bson_error_t that holds error information in case of failure
 *
 * @return 1 if the modification was performed successfully; -1 otherwise.
 */
int
monary_set_column_item(monary_column_data * coldata,
                       unsigned int colnum,
                       const char *field,
                       unsigned int type,
                       unsigned int type_arg,
                       void *storage, unsigned char *mask, bson_error_t * err)
{
    int len;

    monary_column_item *col;

    if (coldata == NULL) {
        monary_error(err, "null argument passed to monary_set_column_item: "
                     "coldata");
        return -1;
    }
    if (colnum >= coldata->num_columns) {
        monary_error(err, "colnum exceeded number of columns in "
                     "monary_set_column_item");
        return -1;
    }
    if (type == TYPE_UNDEFINED || type > LAST_TYPE) {
        monary_error(err, "column type passed to monary_set_column_item was "
                     "undefined");
        return -1;
    }
    if (storage == NULL) {
        monary_error(err, "null argument passed to monary_set_column_item: "
                     "storage");
        return -1;
    }
    if (mask == NULL) {
        monary_error(err, "null argument passed to monary_set_column_item: "
                     "mask");
        return -1;
    }

    len = strlen(field);
    if (len > MONARY_MAX_STRING_LENGTH) {
        monary_error(err, "field name length exceeded maximum in "
                     "monary_set_column_item");
        return -1;
    }

    col = coldata->columns + colnum;

    col->field = malloc(len + 1);
    strcpy(col->field, field);

    col->type = type;
    col->type_arg = type_arg;
    col->storage = storage;
    col->mask = mask;

    return 1;
}

int
monary_load_objectid_value(const bson_iter_t * bsonit,
                           monary_column_item * citem, int idx)
{
    const bson_oid_t *oid;

    uint8_t *dest;

    if (BSON_ITER_HOLDS_OID(bsonit)) {
        oid = bson_iter_oid(bsonit);
        dest = ((uint8_t *) citem->storage) + (idx * sizeof(bson_oid_t));
        memcpy(dest, oid->bytes, sizeof(bson_oid_t));
        return 1;
    }
    else {
        return 0;
    }
}

int
monary_load_bool_value(const bson_iter_t * bsonit,
                       monary_column_item * citem, int idx)
{
    bool value;

    value = bson_iter_bool(bsonit);
    memcpy(((bool *) citem->storage) + idx, &value, sizeof(bool));
    return 1;
}

#define MONARY_DEFINE_FLOAT_LOADER(FUNCNAME, NUMTYPE)                        \
int FUNCNAME (const bson_iter_t* bsonit,                                     \
              monary_column_item* citem,                                     \
              int idx)                                                       \
{                                                                            \
    NUMTYPE value;                                                           \
    if (BSON_ITER_HOLDS_DOUBLE(bsonit)) {                                    \
        value = (NUMTYPE) bson_iter_double(bsonit);                          \
        memcpy(((NUMTYPE*) citem->storage) + idx, &value, sizeof(NUMTYPE));  \
        return 1;                                                            \
    } else if (BSON_ITER_HOLDS_INT32(bsonit)) {                              \
        value = (NUMTYPE) bson_iter_int32(bsonit);                           \
        memcpy(((NUMTYPE*) citem->storage) + idx, &value, sizeof(NUMTYPE));  \
        return 1;                                                            \
    } else if (BSON_ITER_HOLDS_INT64(bsonit)) {                              \
        value = (NUMTYPE) bson_iter_int64(bsonit);                           \
        memcpy(((NUMTYPE*) citem->storage) + idx, &value, sizeof(NUMTYPE));  \
        return 1;                                                            \
    } else {                                                                 \
        return 0;                                                            \
    }                                                                        \
}

// Floating point
MONARY_DEFINE_FLOAT_LOADER(monary_load_float32_value, float)
MONARY_DEFINE_FLOAT_LOADER(monary_load_float64_value, double)

#define MONARY_DEFINE_INT_LOADER(FUNCNAME, NUMTYPE)                          \
int FUNCNAME (const bson_iter_t* bsonit,                                     \
              monary_column_item* citem,                                     \
              int idx)                                                       \
{                                                                            \
    NUMTYPE value;                                                           \
    if (BSON_ITER_HOLDS_INT32(bsonit)) {                                     \
        value = (NUMTYPE) bson_iter_int32(bsonit);                           \
        memcpy(((NUMTYPE*) citem->storage) + idx, &value, sizeof(NUMTYPE));  \
        return 1;                                                            \
    } else if (BSON_ITER_HOLDS_INT64(bsonit)) {                              \
        value = (NUMTYPE) bson_iter_int64(bsonit);                           \
        memcpy(((NUMTYPE*) citem->storage) + idx, &value, sizeof(NUMTYPE));  \
        return 1;                                                            \
    } else if (BSON_ITER_HOLDS_DOUBLE(bsonit)) {                             \
        value = (NUMTYPE) bson_iter_double(bsonit);                          \
        memcpy(((NUMTYPE*) citem->storage) + idx, &value, sizeof(NUMTYPE));  \
        return 1;                                                            \
    } else {                                                                 \
        return 0;                                                            \
    }                                                                        \
}
// Signed integers
MONARY_DEFINE_INT_LOADER(monary_load_int8_value, int8_t)
MONARY_DEFINE_INT_LOADER(monary_load_int16_value, int16_t)
MONARY_DEFINE_INT_LOADER(monary_load_int32_value, int32_t)
MONARY_DEFINE_INT_LOADER(monary_load_int64_value, int64_t)
// Unsigned integers
MONARY_DEFINE_INT_LOADER(monary_load_uint8_value, uint8_t)
MONARY_DEFINE_INT_LOADER(monary_load_uint16_value, uint16_t)
MONARY_DEFINE_INT_LOADER(monary_load_uint32_value, uint32_t)
MONARY_DEFINE_INT_LOADER(monary_load_uint64_value, uint64_t)

int monary_load_datetime_value(const bson_iter_t * bsonit,
                                    monary_column_item * citem, int idx)
{
    int64_t value;

    if (BSON_ITER_HOLDS_DATE_TIME(bsonit)) {
        value = bson_iter_date_time(bsonit);
        memcpy(((int64_t *) citem->storage) + idx, &value, sizeof(int64_t));
        return 1;
    }
    else {
        return 0;
    }
}

int
monary_load_timestamp_value(const bson_iter_t * bsonit,
                            monary_column_item * citem, int idx)
{
    uint32_t timestamp;

    uint32_t increment;

    char *dest;                 // Would be void*, but Windows compilers complain

    dest = (char *)citem->storage + idx * sizeof(int64_t);
    if (BSON_ITER_HOLDS_TIMESTAMP(bsonit)) {
        bson_iter_timestamp(bsonit, &timestamp, &increment);
        memcpy(dest, &timestamp, sizeof(int32_t));
        memcpy(dest + sizeof(int32_t), &increment, sizeof(int32_t));
        return 1;
    }
    else {
        return 0;
    }
}

int
monary_load_string_value(const bson_iter_t * bsonit,
                         monary_column_item * citem, int idx)
{
    char *dest;                 // Pointer to the final location of the array in mem

    const char *src;            // Pointer to immutable buffer

    int size;

    uint32_t stringlen;         // The size of the string according to iter_utf8

    if (BSON_ITER_HOLDS_UTF8(bsonit)) {
        src = bson_iter_utf8(bsonit, &stringlen);
        size = citem->type_arg;
        if (stringlen > size) {
            stringlen = size;
        }
        dest = ((char *)citem->storage) + (idx * size);
        // Note: numpy strings need not end in \0
        memcpy(dest, src, stringlen);
        return 1;
    }
    else {
        return 0;
    }
}

int
monary_load_binary_value(const bson_iter_t * bsonit,
                         monary_column_item * citem, int idx)
{
    bson_subtype_t subtype;

    const uint8_t *binary;

    int size;

    uint32_t binary_len;

    uint8_t *dest;

    if (BSON_ITER_HOLDS_BINARY(bsonit)) {
        // Load the binary
        bson_iter_binary(bsonit, &subtype, &binary_len, &binary);

        // Size checking
        size = citem->type_arg;
        if (binary_len > size) {
            binary_len = size;
        }

        dest = ((uint8_t *) citem->storage) + (idx * size);
        memcpy(dest, binary, binary_len);
        return 1;
    }
    else {
        return 0;
    }
}

int
monary_load_document_value(const bson_iter_t * bsonit,
                           monary_column_item * citem, int idx)
{
    uint32_t document_len;      // The length of document in bytes.

    const uint8_t *document;    // Pointer to the immutable document buffer.

    uint8_t *dest;

    if (BSON_ITER_HOLDS_DOCUMENT(bsonit)) {
        bson_iter_document(bsonit, &document_len, &document);
        if (document_len > citem->type_arg) {
            document_len = citem->type_arg;
        }

        dest = ((uint8_t *) citem->storage) + (idx * document_len);
        memcpy(dest, document, document_len);
        return 1;
    }
    else {
        return 0;
    }
}

int
monary_load_type_value(const bson_iter_t * bsonit,
                       monary_column_item * citem, int idx)
{
    uint8_t type;

    uint8_t *dest;

    type = (uint8_t) bson_iter_type(bsonit);
    dest = ((uint8_t *) citem->storage) + idx;
    memcpy(dest, &type, sizeof(uint8_t));
    return 1;
}

int
monary_load_size_value(const bson_iter_t * bsonit,
                       monary_column_item * citem, int idx)
{
    bson_type_t type;

    const uint8_t *discard;

    uint32_t size;

    uint32_t *dest;

    type = bson_iter_type(bsonit);
    switch (type) {
    case BSON_TYPE_UTF8:
    case BSON_TYPE_CODE:
        bson_iter_utf8(bsonit, &size);
        break;
    case BSON_TYPE_BINARY:
        bson_iter_binary(bsonit, NULL, &size, &discard);
        break;
    case BSON_TYPE_DOCUMENT:
        bson_iter_document(bsonit, &size, &discard);
        break;
    case BSON_TYPE_ARRAY:
        bson_iter_array(bsonit, &size, &discard);
        break;
    default:
        return 0;
    }
    dest = ((uint32_t *) citem->storage) + idx;
    memcpy(dest, &size, sizeof(uint32_t));
    return 1;
}

int
monary_load_length_value(const bson_iter_t * bsonit,
                         monary_column_item * citem, int idx)
{
    bson_type_t type;

    bson_iter_t child;

    const char *discard;

    uint32_t length;

    uint32_t *dest;

    type = bson_iter_type(bsonit);
    switch (type) {
    case BSON_TYPE_UTF8:
    case BSON_TYPE_CODE:
        discard = bson_iter_utf8(bsonit, &length);
        for (length = 0; *discard; length++) {
            discard = bson_utf8_next_char(discard);
        }
        break;
    case BSON_TYPE_ARRAY:
    case BSON_TYPE_DOCUMENT:
        if (!bson_iter_recurse(bsonit, &child)) {
            return 0;
        }
        for (length = 0; bson_iter_next(&child); length++);
        break;
    case BSON_TYPE_BINARY:
        bson_iter_binary(bsonit, NULL, &length, (const uint8_t **)&discard);
        break;
    default:
        return 0;
    }

    dest = ((uint32_t *) citem->storage) + idx;
    memcpy(dest, &length, sizeof(uint32_t));
    return 1;
}

#define MONARY_DISPATCH_TYPE(TYPENAME, TYPEFUNC)    \
case TYPENAME:                                      \
success = TYPEFUNC(bsonit, citem, offset);          \
break;

int
monary_load_item(const bson_iter_t * bsonit,
                 monary_column_item * citem, int offset)
{
    int success = 0;

    switch (citem->type) {
        MONARY_DISPATCH_TYPE(TYPE_OBJECTID, monary_load_objectid_value)
        MONARY_DISPATCH_TYPE(TYPE_DATE, monary_load_datetime_value)
        MONARY_DISPATCH_TYPE(TYPE_TIMESTAMP, monary_load_timestamp_value)
        MONARY_DISPATCH_TYPE(TYPE_BOOL, monary_load_bool_value)

        MONARY_DISPATCH_TYPE(TYPE_INT8, monary_load_int8_value)
        MONARY_DISPATCH_TYPE(TYPE_INT16, monary_load_int16_value)
        MONARY_DISPATCH_TYPE(TYPE_INT32, monary_load_int32_value)
        MONARY_DISPATCH_TYPE(TYPE_INT64, monary_load_int64_value)

        MONARY_DISPATCH_TYPE(TYPE_UINT8, monary_load_uint8_value)
        MONARY_DISPATCH_TYPE(TYPE_UINT16, monary_load_uint16_value)
        MONARY_DISPATCH_TYPE(TYPE_UINT32, monary_load_uint32_value)
        MONARY_DISPATCH_TYPE(TYPE_UINT64, monary_load_uint64_value)

        MONARY_DISPATCH_TYPE(TYPE_FLOAT32, monary_load_float32_value)
        MONARY_DISPATCH_TYPE(TYPE_FLOAT64, monary_load_float64_value)

        MONARY_DISPATCH_TYPE(TYPE_STRING, monary_load_string_value)
        MONARY_DISPATCH_TYPE(TYPE_BINARY, monary_load_binary_value)
        MONARY_DISPATCH_TYPE(TYPE_BSON, monary_load_document_value)

        MONARY_DISPATCH_TYPE(TYPE_SIZE, monary_load_size_value)
        MONARY_DISPATCH_TYPE(TYPE_LENGTH, monary_load_length_value)
        MONARY_DISPATCH_TYPE(TYPE_TYPE, monary_load_type_value)
    default:
        DEBUG("%s does not match any Monary type", citem->field);
        break;
    }

    return success;
}

/**
 * Copies over raw BSON data into Monary column storage. This function
 * determines the types of the data, dispatches to an appropriate handler and
 * copies over the data. It keeps a count of any unsuccessful loads and sets
 * NumPy-compatible masks on the data as appropriate.
 *
 * @param coldata A pointer to monary_column_data which contains the final
 * storage location for the BSON data.
 * @param row The row number to store the data in. Cannot exceed
 * coldata->num_rows.
 * @param bson_data A pointer to an immutable BSON data buffer.
 *
 * @return The number of unsuccessful loads.
 */
int
monary_bson_to_arrays(monary_column_data * coldata,
                      unsigned int row, const bson_t * bson_data)
{
    bson_iter_t bsonit;

    bson_iter_t descendant;

    int i;

    int masked;

    int success;

    monary_column_item *citem;

    if (!coldata || !bson_data) {
        DEBUG("%s",
              "Array pointer or BSON data was NULL and could not be loaded.");
        return -1;
    }
    if (row > coldata->num_rows) {
        DEBUG
            ("Tried to load row %d, but that exceeds the maximum # of rows (%d) ",
             row, coldata->num_rows);
        return -1;
    }

    masked = 0;
    for (i = 0; i < coldata->num_columns; i++) {
        success = 0;
        citem = coldata->columns + i;

        // Use the iterator to find the field we want
        bson_iter_init(&bsonit, bson_data);
        if (bson_iter_find_descendant(&bsonit, citem->field, &descendant)) {
            success = monary_load_item(&descendant, citem, row);
        }

        // Record success in mask
        if (citem->mask != NULL) {
            citem->mask[row] = !success;
        }
        if (!success) {
            masked++;
        }
    }

    return masked;
}

/**
 * Performs a count query on a MongoDB collection.
 *
 * @param collection The MongoDB collection to query against.
 * @param query A pointer to a BSON buffer representing the query.
 * @param err bson_error_t that holds error information in case of failure
 *
 * @return If unsuccessful, returns -1; otherwise, returns the number of
 * documents counted.
 */
int64_t
monary_query_count(mongoc_collection_t * collection,
                   const uint8_t * query, bson_error_t * err)
{
    bson_t query_bson;          // The query converted to BSON format

    int64_t total_count;        // The number of documents counted

    uint32_t query_size;        // Length of the query in bytes

    DEBUG("%s", "Starting Monary count");

    // build BSON query data
    memcpy(&query_size, query, sizeof(uint32_t));
    if (!bson_init_static(&query_bson, query, query_size)) {
        monary_error(err, "failed to initialize raw BSON query in"
                     "monary_query_count");
        return -1;
    }

    // Make the count query
    total_count = mongoc_collection_count(collection,
                                          MONGOC_QUERY_NONE,
                                          &query_bson, 0, 0, NULL, err);
    bson_destroy(&query_bson);
    if (total_count < 0) {
        DEBUG("error: %d.%d %s", err->domain, err->code, err->message);
    }

    return total_count;
}

/**
 * Given pre-allocated array data that specifies the fields to find, this
 * builds a BSON document that can be passed into a MongoDB query.
 *
 * @param coldata A pointer to a monary_column_data, which should have already
 * been allocated and built properly. The names of the fields of its column
 * items become the names of the fields to query for.
 * @param fields_bson A pointer to a bson_t that should already be initialized.
 * After this BSON is written to, it may be used in a query and then destroyed
 * afterwards.
 */
void
monary_get_bson_fields_list(monary_column_data * coldata, bson_t * fields_bson)
{
    int i;

    monary_column_item *col;

    // We want to select exactly each field specified in coldata, of which
    // there are exactly coldata.num_columns
    for (i = 0; i < coldata->num_columns; i++) {
        col = coldata->columns + i;
        bson_append_int32(fields_bson, col->field, -1, 1);
    }
}

/**
 * Performs a find query on a MongoDB collection, selecting certain fields from
 * the results and storing them in Monary columns.
 *
 * @param collection The MongoDB collection to query against.
 * @param offset The number of documents to skip, or zero.
 * @param limit The maximum number of documents to return, or zero.
 * @param query A pointer to a BSON buffer representing the query.
 * @param coldata The column data to store the results in.
 * @param select_fields If truthy, select exactly the fields from the database
 * that match the fields in coldata. If false, the query will find and return
 * all fields from matching documents.
 * @param err bson_error_t that holds error information in case of failure
 *
 * @return If successful, a Monary cursor that should be freed with
 * monary_close_query() when no longer in use. If unsuccessful, or if an
 * invalid query was passed in, NULL is returned.
 */
monary_cursor *
monary_init_query(mongoc_collection_t * collection,
                  uint32_t offset,
                  uint32_t limit,
                  const uint8_t * query,
                  monary_column_data * coldata,
                  int select_fields, bson_error_t * err)
{
    bson_t query_bson;          // BSON representing the query to perform

    bson_t *fields_bson;        // BSON holding the fields to select

    int32_t query_size;

    monary_cursor *cursor;

    mongoc_cursor_t *mcursor;   // A MongoDB cursor

    // Sanity checks
    if (!collection || !query || !coldata) {
        monary_error(err, "null parameter passed to monary_init_query");
        return NULL;
    }

    // build BSON query data
    memcpy(&query_size, query, sizeof(int32_t));
    query_size = (int32_t) BSON_UINT32_FROM_LE(query_size);
    if (!bson_init_static(&query_bson, query, query_size)) {
        monary_error(err, "failed to initialize raw bson query in "
                     "monary_init_query");
        return NULL;
    }
    fields_bson = NULL;

    // build BSON fields list (if necessary)
    if (select_fields) {
        fields_bson = bson_new();
        if (!fields_bson) {
            monary_error(err,
                         "error occurred while allocating memory for BSON "
                         "data in monary_init_query");
            return NULL;
        }
        monary_get_bson_fields_list(coldata, fields_bson);
    }

    // create query cursor
    mcursor = mongoc_collection_find(collection,
                                     MONGOC_QUERY_NONE,
                                     offset,
                                     limit, 0, &query_bson, fields_bson, NULL);

    // destroy BSON fields
    bson_destroy(&query_bson);
    if (fields_bson) {
        bson_destroy(fields_bson);
    }

    if (!mcursor) {
        monary_error(err, "error occurred within mongoc_collection_find in "
                     "monary_init_query");
        return NULL;
    }

    // finally, create a new Monary cursor
    cursor = (monary_cursor *) malloc(sizeof(monary_cursor));
    cursor->mcursor = mcursor;
    cursor->coldata = coldata;
    return cursor;
}

/**
 * Performs an aggregation operation on a MongoDB collection.
 *
 * @param collection The MongoDB collection to query against.
 * @param pipeline A pointer to a BSON buffer representing the pipeline.
 * @param coldata The column data to store the results in.
 * @param err bson_error_t that holds error information in case of failure
 *
 * @return If successful, a Monary cursor that should be freed with
 * monary_close_query() when no longer in use. If unsuccessful, or if an invalid
 * pipeline was passed in, NULL is returned.
 */
monary_cursor *
monary_init_aggregate(mongoc_collection_t * collection,
                      const uint8_t * pipeline,
                      monary_column_data * coldata, bson_error_t * err)
{
    bson_t pl_bson;

    int32_t pl_size;

    mongoc_cursor_t *mcursor;

    monary_cursor *cursor;

    // Sanity checks
    if (!collection) {
        monary_error(err,
                     "invalid collection passed to monary_init_aggregate");
        return NULL;
    }
    else if (!pipeline) {
        monary_error(err, "invalid pipeline passed to monary_init_aggregate");
        return NULL;
    }

    // Build BSON pipeline
    memcpy(&pl_size, pipeline, sizeof(int32_t));
    pl_size = (int32_t) BSON_UINT32_FROM_LE(pl_size);
    if (!bson_init_static(&pl_bson, pipeline, pl_size)) {
        monary_error(err, "failed to initialize raw BSON pipeline in "
                     "monary_init_aggregate");
        return NULL;
    }

    // Get an aggregation cursor
    mcursor = mongoc_collection_aggregate(collection,
                                          MONGOC_QUERY_NONE,
                                          &pl_bson, NULL, NULL);

    // Clean up
    bson_destroy(&pl_bson);

    if (!mcursor) {
        monary_error(err, "error occurred in mongoc_collection_aggregate in "
                     "monary_init_aggregate");
        return NULL;
    }

    cursor = (monary_cursor *) malloc(sizeof(monary_cursor));
    cursor->mcursor = mcursor;
    cursor->coldata = coldata;
    return cursor;
}

/**
 * Grabs the results obtained from the MongoDB cursor and loads them into
 * in-memory arrays.
 *
 * @param cursor A pointer to a Monary cursor, which contains both a MongoDB
 * cursor and Monary column data that stores the retrieved information.
 * @param err bson_error_t that holds error information in case of failure
 *
 * @return The number of rows loaded into memory.
 */
int
monary_load_query(monary_cursor * cursor, bson_error_t * err)
{
    const bson_t *bson;         // Pointer to an immutable BSON buffer

    int num_masked;

    int row;

    int total_values;

    monary_column_data *coldata;

    mongoc_cursor_t *mcursor;

    mcursor = cursor->mcursor;  // The underlying MongoDB cursor
    coldata = cursor->coldata;  // A pointer to the NumPy array data
    row = 0;            // Iterator var over the lengths of the arrays
    num_masked = 0;     // The number of failed loads

    // read result values
    while (row < coldata->num_rows && !mongoc_cursor_error(mcursor, err)
           && mongoc_cursor_next(mcursor, &bson)) {

#ifndef NDEBUG
        if (row % 500000 == 0) {
            DEBUG("...%i rows loaded", row);
        }
#endif

        num_masked += monary_bson_to_arrays(coldata, row, bson);
        ++row;
    }

    if (mongoc_cursor_error(mcursor, err)) {
        return -1;
    }

    total_values = row * coldata->num_columns;
    DEBUG("%i rows loaded; %i / %i values were masked", row, num_masked,
          total_values);

    return row;
}

/**
 * Destroys the underlying MongoDB cursor associated with the given cursor.
 *
 * Note that the column data is not freed in this function as that data is
 * exposed as NumPy arrays in Python.
 *
 * @param cursor A pointer to the Monary cursor to close. If cursor is NULL,
 * no operation is performed.
 */
void
monary_close_query(monary_cursor * cursor)
{
    if (cursor) {
        DEBUG("%s", "Closing query");
        mongoc_cursor_destroy(cursor->mcursor);
        free(cursor);
    }
}

/**
 * Create a write concern pointer to be used for insert, remove, or update.
 *
 * @param write_concern_w The number of nodes that each document must be
 *                        written to before the server acknowledges the write.
 * @param write_concern_wtimeout The number of milliseconds before write
 *                               timeout.
 * @param write_concern_wtag The write concern tag.
 * @param write_concern_journal Whether or not the write request should be
 *                              journaled before acknowledging the write
 *                              request.
 * @param write_concern_fsync Whether or not fsync() should be called on the
 *                            server before acknowledging the write request.
 *
 * @return The newly created write concern.
 */
mongoc_write_concern_t *
monary_create_write_concern(int write_concern_w,
                            int write_concern_wtimeout,
                            bool write_concern_journal,
                            bool write_concern_fsync, char *write_concern_wtag)
{
    mongoc_write_concern_t *write_concern = mongoc_write_concern_new();

    mongoc_write_concern_set_w(write_concern, write_concern_w);
    mongoc_write_concern_set_wtimeout(write_concern, write_concern_wtimeout);
    mongoc_write_concern_set_journal(write_concern, write_concern_journal);
    mongoc_write_concern_set_fsync(write_concern, write_concern_fsync);
    if (write_concern_wtag) {
        mongoc_write_concern_set_wtag(write_concern, write_concern_wtag);
    }

    return write_concern;
}

/**
 * Destroys the write concern, freeing the data.
 *
 * @param write_concern The write concern to be destroyed.
 */
void
monary_destroy_write_concern(mongoc_write_concern_t * write_concern)
{
    mongoc_write_concern_destroy(write_concern);
}

#define MONARY_SET_BSON_VALUE(TYPENAME, BTYPENAME, VKEY, STORED_TYPE, CAST_TYPE) \
case TYPENAME:                                                                   \
val->value_type = BTYPENAME;                                                     \
val->value.VKEY = (CAST_TYPE) *(((STORED_TYPE *) citem->storage) + idx);         \
break;

/**
 * Create a bson_value_t from the given monary column and row
 *
 * @param val A pointer to the bson_value_t to populate.
 * @param citem The monary column that contains the value to use
 * @param idx The index of the storage to use.
 */
void
monary_make_bson_value_t(bson_value_t * val,
                         monary_column_item * citem, int idx)
{
    uint32_t len;

    uint8_t *storage = ((uint8_t *) citem->storage);

    uint8_t *current_val = storage + (idx * citem->type_arg);

    val->padding = 0;
    switch (citem->type) {
        MONARY_SET_BSON_VALUE(TYPE_BOOL, BSON_TYPE_BOOL, v_bool, bool, bool)
        MONARY_SET_BSON_VALUE(TYPE_INT8, BSON_TYPE_INT32,
                              v_int32, int8_t, int32_t)
        MONARY_SET_BSON_VALUE(TYPE_INT16, BSON_TYPE_INT32,
                              v_int32, int16_t, int32_t)
        MONARY_SET_BSON_VALUE(TYPE_INT32, BSON_TYPE_INT32,
                              v_int32, int32_t, int32_t)
        MONARY_SET_BSON_VALUE(TYPE_INT64, BSON_TYPE_INT64,
                              v_int64, int64_t, int64_t)
        MONARY_SET_BSON_VALUE(TYPE_UINT8, BSON_TYPE_INT32,
                              v_int32, uint8_t, int32_t)
        MONARY_SET_BSON_VALUE(TYPE_UINT16, BSON_TYPE_INT32,
                              v_int32, uint16_t, int32_t)
        MONARY_SET_BSON_VALUE(TYPE_UINT32, BSON_TYPE_INT32,
                              v_int32, uint32_t, int32_t)
        MONARY_SET_BSON_VALUE(TYPE_UINT64, BSON_TYPE_INT64,
                              v_int64, uint64_t, int64_t)
        MONARY_SET_BSON_VALUE(TYPE_FLOAT32, BSON_TYPE_DOUBLE,
                              v_double, float, double)

        MONARY_SET_BSON_VALUE(TYPE_FLOAT64, BSON_TYPE_DOUBLE,
                              v_double, double, double)

        MONARY_SET_BSON_VALUE(TYPE_DATE, BSON_TYPE_DATE_TIME,
                              v_datetime, int64_t, int64_t)
        case TYPE_OBJECTID:val->value_type = BSON_TYPE_OID;

        bson_oid_init_from_data(&(val->value.v_oid),
                                storage + (idx * sizeof(bson_oid_t)));
        break;
    case TYPE_TIMESTAMP:
        val->value_type = BSON_TYPE_TIMESTAMP;
        memcpy(&val->value.v_timestamp.timestamp,
               ((uint32_t *) citem->storage) + (2 * idx), sizeof(uint32_t));
        memcpy(&val->value.v_timestamp.increment,
               ((uint32_t *) citem->storage) + (2 * idx + 1),
               sizeof(uint32_t));
        break;
    case TYPE_STRING:
        val->value_type = BSON_TYPE_UTF8;
        val->value.v_utf8.len = citem->type_arg;
        val->value.v_utf8.str = (char *)current_val;
        break;
    case TYPE_BINARY:
        val->value_type = BSON_TYPE_BINARY;
        val->value.v_binary.subtype = BSON_SUBTYPE_BINARY;
        val->value.v_binary.data_len = citem->type_arg;
        val->value.v_binary.data = current_val;
        break;
    case TYPE_BSON:
        // The first 4 bytes of the bson is the length.
        len = BSON_UINT32_FROM_LE(*(uint32_t *) current_val);
        if (len > citem->type_arg) {
            DEBUG("Error: bson length greater than array width in "
                  "row %d", idx);
            break;
        }
        if (len < 5) {
            DEBUG("Error: poorly formatted bson in row %d", idx);
            break;
        }
        val->value_type = BSON_TYPE_DOCUMENT;
        val->value.v_doc.data_len = len;
        val->value.v_doc.data = current_val;
        break;
    default:
        DEBUG("Unsupported type %d", citem->type);
        break;
    }
}

/**
 * Creates the bson document @parent from the given columns.
 *
 * @param columns A list of monary column items storing the values to insert
 * @param row The row in which the current data is stored
 * @param col_start The column at which to start when appending values
 * @param col_end The column at which to end when appending values
 * @param parent The bson document to append to
 * @param name_offset Offset into the field name (for nested documents)
 * @param depth Number of recursive calls made
 */
void
monary_bson_from_columns(monary_column_item * columns,
                         int row,
                         int col_start,
                         int col_end,
                         bson_t * parent, int name_offset, int depth)
{
    bson_t child;

    bson_value_t val;

    char *field;

    monary_column_item *citem;

    int dot_idx;

    int i;

    int new_end;

    if (depth >= MONARY_MAX_RECURSION) {
        DEBUG("Max recursive depth (%d) exceed on row: %d",
              MONARY_MAX_RECURSION, row);
        return;
    }
    for (i = col_start; i < col_end; i++) {
        citem = columns + i;
        if (!*(citem->mask + row)) {
            // only append unmasked values
            dot_idx = 0;
            for (field = citem->field + name_offset; *(field + dot_idx) && (field[dot_idx] != '.'); dot_idx++); // Advance dot_idx to either '.' or '\0'
            if (*(field + dot_idx)) {
                // Here we will have a nested document
                new_end = i + 1;
                // This while loop will advance new_end so every column between
                // i and new_end have the same key up until the '.'
                while (new_end < col_end) {
                    // Check that the field we're looking at is long enough to
                    // be at the same nested level as ``field``.
                    if (strlen((columns + new_end)->field) >
                        name_offset + dot_idx) {
                        // Check that the field we're looking at is the same
                        // as ``field`` up until the '.'
                        if (strncmp(field,
                                    (columns + new_end)->field + name_offset,
                                    dot_idx) == 0) {
                            new_end++;
                            continue;
                        }
                    }
                    break;
                }
                bson_append_document_begin(parent, citem->field + name_offset,
                                           dot_idx, &child);
                monary_bson_from_columns(columns, row, i, new_end, &child,
                                         name_offset + dot_idx + 1, depth + 1);
                bson_append_document_end(parent, &child);
                i = new_end - 1;
            }
            else {
                // No nested document in this else case
                monary_make_bson_value_t(&val, citem, row);
                bson_append_value(parent, citem->field + name_offset,
                                  dot_idx, &val);
            }
        }
    }
}

/**
 * Mask all indices that the server says had an error.
 *
 * @param errors A bson_iter containing the array of errors.
 * @param mask A buffer representing the mask.
 * @param offset The offset into the mask at which to start writing.
 *
 * @return number masked if successful, -1 otherwise
 */
int
monary_mask_failed_writes(bson_iter_t * errors,
                          unsigned char *mask, int offset)
{
    bson_iter_t array_iter;

    bson_iter_t document_iter;

    int index;

    int num_masked = 0;

    if (!BSON_ITER_HOLDS_ARRAY(errors)) {
        return -1;
    }
    if (!bson_iter_recurse(errors, &array_iter)) {
        return -1;
    }
    while (bson_iter_next(&array_iter)) {
        if (bson_iter_recurse(&array_iter, &document_iter) &&
            bson_iter_find(&document_iter, "index")) {
            if (BSON_ITER_HOLDS_INT32(&document_iter)) {
                index = bson_iter_int32(&document_iter);
            }
            else {
                return -1;
            }
            mask[index + offset] = 1;
            num_masked++;
        }
        else {
            return -1;
        }
    }
    return num_masked;
}

/**
 * Puts the given data into BSON and inserts into the given collection.
 *
 * @param collection The MongoDB collection to insert to.
 * @param coldata The column data storing the values to insert.
 * @param id_data The column data that will return the generated object ids,
 *                or Null if the '_id' field has been provided.
 * @param client The connection to the database.
 * @param write_concern The write concern to be used for these inserts.
 * @param err bson_error_t that holds error information in case of failure
 */
void
monary_insert(mongoc_collection_t * collection,
              monary_column_data * coldata,
              monary_column_data * id_data,
              mongoc_client_t * client,
              mongoc_write_concern_t * write_concern, bson_error_t * err)
{
    bson_iter_t bsonit;

    bson_oid_t oid;

    bson_t document;

    bson_t reply;

    monary_column_item *citem;

    mongoc_bulk_operation_t *bulk_op;

    bool id_provided;

    char *str;

    int data_len;

    int i;

    int max_message_size;

    int num_docs;

    int num_inserted;

    int num_processed;

    int row;

    uint8_t *storage;

    // Sanity checks
    if (!collection || !coldata || !id_data) {
        DEBUG("%s", "Given a NULL param.");
        return;
    }

    bulk_op = mongoc_collection_create_bulk_operation(collection, false,
                                                      write_concern);

    bson_init(&document);
    bson_init(&reply);
    num_inserted = 0;
    num_processed = 0;

    max_message_size = mongoc_client_get_max_message_size(client);
    DEBUG("Max message size: %d", max_message_size);
    data_len = 0;

    id_provided = (strcmp(coldata->columns->field, "_id") == 0);

    // Generate all ObjectId's in advance if the user has not specified "_id"
    if (!id_provided) {
        storage = id_data->columns->storage;
        for (i = 0; i < coldata->num_rows; i++) {
            bson_oid_init(&oid, NULL);
            memcpy(storage, oid.bytes, sizeof(bson_oid_t));
            // Move the storage pointer to the next 12 bytes.
            storage += sizeof(bson_oid_t);
        }
        // Reset the storage pointer to the beginning.
        storage = id_data->columns->storage;
    }

    DEBUG("Inserting %d documents with %d keys.",
          coldata->num_rows, coldata->num_columns);
    for (row = 0; row < coldata->num_rows; row++) {
        if (!id_provided) {
            // If _id is not provided, append the generated ObjectId.
            bson_oid_init_from_data(&oid,
                                    storage + (row * sizeof(bson_oid_t)));
            BSON_APPEND_OID(&document, "_id", &oid);
        }
        monary_bson_from_columns(coldata->columns, row, 0,
                                 coldata->num_columns, &document, 0, 0);
        data_len += document.len;
        mongoc_bulk_operation_insert(bulk_op, &document);
        bson_reinit(&document);

        // The C driver sends insert commands or OP_INSERT, depending on
        // server version, in as few batches as possible. Based on our 48M
        // max_message_size, roughly 1 batch for OP_INSERT, roughly 3 for
        // insert commands.
        if (data_len > max_message_size || row == (coldata->num_rows - 1)) {
            num_docs = row + 1 - num_processed;
            // Unmask the values that will be inserted.
            for (i = num_processed; i <= row; i++) {
                (id_data->columns->mask)[i] = 0;
            }
            DEBUG("Inserting documents %d through %d, total data: %d",
                  num_processed + 1, row + 1, data_len);
            if (mongoc_bulk_operation_execute(bulk_op, &reply, err)) {
                num_inserted += num_docs;
                data_len = 0;
            }
            else {
                DEBUG("Error message: %s", err->message);
#ifndef NDEBUG
                str = bson_as_json(&reply, NULL);
                DEBUG("Server reply: %s", str);
                bson_free(str);
#endif
                // Mask all of the values that failed.
                if (bson_iter_init_find(&bsonit, &reply, "writeErrors")) {
                    i = monary_mask_failed_writes(&bsonit,
                                                  id_data->columns->mask,
                                                  num_processed);
                    if (i != -1) {
                        num_inserted += num_docs - i;
                    }
                    else {
                        // If the document masking failed (from a bad server
                        // reply) then mask everything that we tried to write.
                        for (i = num_processed; i <= row; i++) {
                            (id_data->columns->mask)[i] = 1;
                        }
                    }
                }
                else {
                    DEBUG("%s", "Server reply did not contain writeErrors");
                    for (i = num_processed; i <= row; i++) {
                        (id_data->columns->mask)[i] = 1;
                    }
                    goto end;
                }
            }
            num_processed += num_docs;
            mongoc_bulk_operation_destroy(bulk_op);
            bulk_op = mongoc_collection_create_bulk_operation(collection,
                                                              false,
                                                              write_concern);
            bson_reinit(&reply);
        }
    }
  end:
    DEBUG("Inserted %d of %d documents", num_inserted, num_processed);
    bson_destroy(&document);
    bson_destroy(&reply);
    mongoc_bulk_operation_destroy(bulk_op);
}
