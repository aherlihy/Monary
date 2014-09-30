/*
 * Copyright 2013 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include "mongoc-cursor.h"
#include "mongoc-cursor-array-private.h"
#include "mongoc-cursor-private.h"
#include "mongoc-client-private.h"
#include "mongoc-counters-private.h"
#include "mongoc-error.h"
#include "mongoc-log.h"
#include "mongoc-opcode.h"
#include "mongoc-trace.h"


#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "cursor-array"


typedef struct
{
   const bson_t       *result;
   bool         has_array;
   bson_iter_t         iter;
   bson_t              bson;
   uint32_t       document_len;
   const uint8_t *document;
} mongoc_cursor_array_t;


static void *
_mongoc_cursor_array_new (void)
{
   mongoc_cursor_array_t *arr;

   ENTRY;

   arr = bson_malloc0 (sizeof *arr);

   RETURN (arr);
}


static void
_mongoc_cursor_array_destroy (mongoc_cursor_t *cursor)
{
   ENTRY;

   bson_free (cursor->iface_data);
   _mongoc_cursor_destroy (cursor);

   EXIT;
}


static bool
_mongoc_cursor_array_next (mongoc_cursor_t *cursor,
                           const bson_t   **bson)
{
   bool ret = true;
   mongoc_cursor_array_t *arr;
   bson_iter_t iter;

   ENTRY;

   arr = cursor->iface_data;
   *bson = NULL;

   if (!arr->has_array) {
      arr->has_array = true;

      ret = _mongoc_cursor_next (cursor, &arr->result);

      if (!(ret &&
            bson_iter_init_find (&iter, arr->result, "result") &&
            BSON_ITER_HOLDS_ARRAY (&iter) &&
            bson_iter_recurse (&iter, &arr->iter) &&
            bson_iter_next (&arr->iter))) {
         ret = false;
      }
   } else {
      ret = bson_iter_next (&arr->iter);
   }

   if (ret) {
      bson_iter_document (&arr->iter, &arr->document_len, &arr->document);
      bson_init_static (&arr->bson, arr->document, arr->document_len);

      *bson = &arr->bson;
   }

   RETURN (ret);
}


static mongoc_cursor_t *
_mongoc_cursor_array_clone (const mongoc_cursor_t *cursor)
{
   mongoc_cursor_t *clone_;

   ENTRY;

   clone_ = _mongoc_cursor_clone (cursor);
   _mongoc_cursor_array_init (clone_);

   RETURN (clone_);
}


static bool
_mongoc_cursor_array_more (mongoc_cursor_t *cursor)
{
   bool ret;
   mongoc_cursor_array_t *arr;
   bson_iter_t iter;

   ENTRY;

   arr = cursor->iface_data;

   if (arr->has_array) {
      memcpy (&iter, &arr->iter, sizeof iter);

      ret = bson_iter_next (&iter);
   } else {
      ret = true;
   }

   RETURN (ret);
}


static mongoc_cursor_interface_t gMongocCursorArray = {
   _mongoc_cursor_array_clone,
   _mongoc_cursor_array_destroy,
   _mongoc_cursor_array_more,
   _mongoc_cursor_array_next,
};


void
_mongoc_cursor_array_init (mongoc_cursor_t *cursor)
{
   ENTRY;

   cursor->iface_data = _mongoc_cursor_array_new ();

   memcpy (&cursor->iface, &gMongocCursorArray,
           sizeof (mongoc_cursor_interface_t));

   EXIT;
}


