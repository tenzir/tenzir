/*  Minimal Fluent-Bit Header
 *
 *  This header exposes only the symbols needed to use the public
 *  advertised API of libfluent-bit.so
 *
 *  This is derived from fluent-bit/flb_lib.h which is
 *  licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#ifndef FLUENT_BIT_MINIMAL_H
#define FLUENT_BIT_MINIMAL_H

#include <stddef.h>
#include <string.h>

// #include <fluent-bit/flb_macros.h>
// #include <fluent-bit/flb_config.h>
#define FLB_EXPORT extern "C"

/* Lib engine status */
#define FLB_LIB_ERROR -1
#define FLB_LIB_NONE 0
#define FLB_LIB_OK 1
#define FLB_LIB_NO_CONFIG_MAP 2

struct flb_lib_ctx {
    int status;
    void *event_loop;
    void *event_channel;
    void *config;
};

/* Used on out_lib to define a callback and further opaque data */
struct flb_lib_out_cb {
  int (*cb)(void* record, size_t size, void* data);
  void* data;
};

/* For Fluent Bit library callers, we only export the following symbols */
typedef struct flb_lib_ctx flb_ctx_t;

FLB_EXPORT void flb_init_env();
FLB_EXPORT flb_ctx_t* flb_create();
FLB_EXPORT void flb_destroy(flb_ctx_t* ctx);
FLB_EXPORT int flb_input(flb_ctx_t* ctx, const char* input, void* data);
FLB_EXPORT int
flb_output(flb_ctx_t* ctx, const char* output, struct flb_lib_out_cb* cb);
FLB_EXPORT int flb_filter(flb_ctx_t* ctx, const char* filter, void* data);
FLB_EXPORT int flb_input_set(flb_ctx_t* ctx, int ffd, ...);
FLB_EXPORT int
flb_input_property_check(flb_ctx_t* ctx, int ffd, char* key, char* val);
FLB_EXPORT int
flb_output_property_check(flb_ctx_t* ctx, int ffd, char* key, char* val);
FLB_EXPORT int
flb_filter_property_check(flb_ctx_t* ctx, int ffd, char* key, char* val);
FLB_EXPORT int flb_output_set(flb_ctx_t* ctx, int ffd, ...);
FLB_EXPORT int
flb_output_set_test(flb_ctx_t* ctx, int ffd, char* test_name,
                    void (*out_callback)(void*, int, int, void*, size_t, void*),
                    void* out_callback_data, void* test_ctx);
FLB_EXPORT int flb_output_set_callback(flb_ctx_t* ctx, int ffd, char* name,
                                       void (*cb)(char*, void*, void*));

FLB_EXPORT int flb_filter_set(flb_ctx_t* ctx, int ffd, ...);
FLB_EXPORT int flb_service_set(flb_ctx_t* ctx, ...);
FLB_EXPORT int flb_lib_free(void* data);
FLB_EXPORT double flb_time_now();

/* start stop the engine */
FLB_EXPORT int flb_start(flb_ctx_t* ctx);
FLB_EXPORT int flb_stop(flb_ctx_t* ctx);
FLB_EXPORT int flb_loop(flb_ctx_t* ctx);

/* data ingestion for "lib" input instance */
FLB_EXPORT int
flb_lib_push(flb_ctx_t* ctx, int ffd, const void* data, size_t len);
FLB_EXPORT int flb_lib_config_file(flb_ctx_t* ctx, const char* path);

/*********** MsgPack *************/

/**
 * @defgroup msgpack_object Dynamically typed object
 * @ingroup msgpack
 * @{
 */

typedef enum {
  MSGPACK_OBJECT_NIL = 0x00,
  MSGPACK_OBJECT_BOOLEAN = 0x01,
  MSGPACK_OBJECT_POSITIVE_INTEGER = 0x02,
  MSGPACK_OBJECT_NEGATIVE_INTEGER = 0x03,
  MSGPACK_OBJECT_FLOAT32 = 0x0a,
  MSGPACK_OBJECT_FLOAT64 = 0x04,
  MSGPACK_OBJECT_FLOAT = 0x04,
#if defined(MSGPACK_USE_LEGACY_NAME_AS_FLOAT)
  MSGPACK_OBJECT_DOUBLE = MSGPACK_OBJECT_FLOAT, /* obsolete */
#endif /* MSGPACK_USE_LEGACY_NAME_AS_FLOAT */
  MSGPACK_OBJECT_STR = 0x05,
  MSGPACK_OBJECT_ARRAY = 0x06,
  MSGPACK_OBJECT_MAP = 0x07,
  MSGPACK_OBJECT_BIN = 0x08,
  MSGPACK_OBJECT_EXT = 0x09
} msgpack_object_type;

struct msgpack_object;
struct msgpack_object_kv;

typedef struct {
  uint32_t size;
  struct msgpack_object* ptr;
} msgpack_object_array;

typedef struct {
  uint32_t size;
  struct msgpack_object_kv* ptr;
} msgpack_object_map;

typedef struct {
  uint32_t size;
  const char* ptr;
} msgpack_object_str;

typedef struct {
  uint32_t size;
  const char* ptr;
} msgpack_object_bin;

typedef struct {
  int8_t type;
  uint32_t size;
  const char* ptr;
} msgpack_object_ext;

typedef union {
  bool boolean;
  uint64_t u64;
  int64_t i64;
#if defined(MSGPACK_USE_LEGACY_NAME_AS_FLOAT)
  double dec; /* obsolete*/
#endif        /* MSGPACK_USE_LEGACY_NAME_AS_FLOAT */
  double f64;
  msgpack_object_array array;
  msgpack_object_map map;
  msgpack_object_str str;
  msgpack_object_bin bin;
  msgpack_object_ext ext;
} msgpack_object_union;

typedef struct msgpack_object {
  msgpack_object_type type;
  msgpack_object_union via;
} msgpack_object;

typedef struct msgpack_object_kv {
  msgpack_object key;
  msgpack_object val;
} msgpack_object_kv;

/**
 * @defgroup msgpack_zone Memory zone
 * @ingroup msgpack
 * @{
 */

typedef struct msgpack_zone_finalizer {
  void (*func)(void* data);
  void* data;
} msgpack_zone_finalizer;

typedef struct msgpack_zone_finalizer_array {
  msgpack_zone_finalizer* tail;
  msgpack_zone_finalizer* end;
  msgpack_zone_finalizer* array;
} msgpack_zone_finalizer_array;

struct msgpack_zone_chunk;
typedef struct msgpack_zone_chunk msgpack_zone_chunk;

typedef struct msgpack_zone_chunk_list {
  size_t free;
  char* ptr;
  msgpack_zone_chunk* head;
} msgpack_zone_chunk_list;

typedef struct msgpack_zone {
  msgpack_zone_chunk_list chunk_list;
  msgpack_zone_finalizer_array finalizer_array;
  size_t chunk_size;
} msgpack_zone;

/**
 * @defgroup msgpack_unpack Deserializer
 * @ingroup msgpack
 * @{
 */

#ifdef __cplusplus
extern "C" {
#endif

typedef struct msgpack_unpacked {
  msgpack_zone* zone;
  msgpack_object data;
} msgpack_unpacked;

typedef enum {
  MSGPACK_UNPACK_SUCCESS = 2,
  MSGPACK_UNPACK_EXTRA_BYTES = 1,
  MSGPACK_UNPACK_CONTINUE = 0,
  MSGPACK_UNPACK_PARSE_ERROR = -1,
  MSGPACK_UNPACK_NOMEM_ERROR = -2
} msgpack_unpack_return;

msgpack_unpack_return
msgpack_unpack_next(msgpack_unpacked* result, const char* data, size_t len,
                    size_t* off);

static inline void msgpack_unpacked_init(msgpack_unpacked* result) {
  memset(result, 0, sizeof(msgpack_unpacked));
}

void msgpack_zone_free(msgpack_zone* zone);

static inline void msgpack_unpacked_destroy(msgpack_unpacked* result) {
  if (result->zone != NULL) {
    msgpack_zone_free(result->zone);
    result->zone = NULL;
    memset(&result->data, 0, sizeof(msgpack_object));
  }
}

#ifdef __cplusplus
}
#endif

/** @} */

#endif
