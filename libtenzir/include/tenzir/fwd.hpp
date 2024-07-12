//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/config.hpp" // IWYU pragma: export
#include "tenzir/tql/fwd.hpp"

#include <arrow/util/config.h>
#include <caf/config.hpp>
#include <caf/fwd.hpp>
#include <caf/type_id.hpp>
#include <flatbuffers/base.h>

#include <cstdint>
#include <filesystem>
#include <map>
#include <unordered_map>
#include <vector>

#define TENZIR_ADD_TYPE_ID(type) CAF_ADD_TYPE_ID(tenzir_types, type)

// -- arrow --------------------------------------------------------------------

// Note that this can also be achieved by including <arrow/type_fwd.h>, but
// that header is a fair bit more expensive than forward declaring just the
// types we need forward-declared here. If this ever diverges between Arrow
// versions, consider switching to including that file.

namespace arrow {

class Array;
class ArrayBuilder;
class BooleanType;
class Buffer;
class DataType;
class DoubleType;
class DurationType;
class Field;
class Int64Type;
class ListType;
class MapType;
class MemoryPool;
class RecordBatch;
class Schema;
class StringType;
class StructArray;
class StructType;
class TimestampType;
class UInt64Type;

namespace io {

class RandomAccessFile;

} // namespace io

// For backwards compatibility with versions before Arrow 16, we alias Uri to
// the namespace it was moved to when it stabilized.
#if ARROW_VERSION_MAJOR < 16

namespace internal {
class Uri;
} // namespace internal

namespace util {
using ::arrow::internal::Uri;
} // namespace util

#endif

} // namespace arrow

// -- flatbuffers -------------------------------------------------------------

namespace flatbuffers {

#if (FLATBUFFERS_VERSION_REVISION + (FLATBUFFERS_VERSION_MINOR * 100)          \
     + (FLATBUFFERS_VERSION_MAJOR * 10000))                                    \
  >= 230509

template <bool>
class FlatBufferBuilderImpl;

using FlatBufferBuilder = FlatBufferBuilderImpl<false>;

#else

class FlatBufferBuilder;

#endif

template <class T>
struct Offset;

} // namespace flatbuffers

// -- caf ----------------------------------------------------------------------

namespace caf {

// TODO CAF 0.19. Check if this already implemented by CAF itself.
template <class Slot>
struct inspector_access<inbound_stream_slot<Slot>> {
  template <class Inspector, class T>
  static auto apply(Inspector& f, inbound_stream_slot<T>& x) {
    auto val = x.value();
    auto result = f.apply(val);
    if constexpr (Inspector::is_loading)
      x = inbound_stream_slot<T>{val};
    return result;
  }
};

template <>
struct inspector_access<std::filesystem::path> {
  template <class Inspector>
  static auto apply(Inspector& f, std::filesystem::path& x) {
    auto str = x.string();
    auto result = f.apply(str);
    if constexpr (Inspector::is_loading)
      x = {str};
    return result;
  }
};

namespace detail {

class stringification_inspector;

} // namespace detail

} // namespace caf

// -- tenzir ---------------------------------------------------------------------

namespace tenzir {

class active_store;
class aggregation_function;
class bitmap;
class blob_type;
class bool_type;
class chunk;
class command;
class configuration;
class data;
class diagnostic_builder;
class diagnostic_handler;
class double_type;
class duration_type;
class enumeration_type;
class ewah_bitmap;
class expression;
class http_request_description;
// class http_request;
class int64_type;
class ip_type;
class ip;
class legacy_abstract_type;
class legacy_type;
class list_type;
class map_type;
class module;
class null_bitmap;
class null_type;
class operator_base;
class operator_box;
class parser_interface;
class passive_store;
class pattern;
class pipeline;
class pipeline;
class plugin_ptr;
class plugin;
class port;
class record_type;
class segment;
class shared_diagnostic_handler;
class string_type;
class subnet_type;
class subnet;
class synopsis;
class table_slice_builder;
class table_slice;
class time_type;
class type;
class uint64_type;
class uuid;
class value_index;
class wah_bitmap;

struct accountant_config;
struct active_partition_state;
struct attribute;
struct catalog_lookup_result;
struct component_map_entry;
struct component_map;
struct component_state_map;
struct component_state;
struct concept_;
struct conjunction;
struct connect_request;
struct curried_predicate;
struct data_extractor;
struct data_point;
struct diagnostic;
struct disjunction;
struct extract_query_context;
struct field_extractor;
struct flow;
struct identifier;
struct index_state;
struct invocation;
struct legacy_address_type;
struct legacy_alias_type;
struct legacy_bool_type;
struct legacy_count_type;
struct legacy_duration_type;
struct legacy_enumeration_type;
struct legacy_integer_type;
struct legacy_list_type;
struct legacy_map_type;
struct legacy_none_type;
struct legacy_pattern_type;
struct legacy_real_type;
struct legacy_record_type;
struct legacy_string_type;
struct legacy_subnet_type;
struct legacy_time_type;
struct location;
struct measurement;
struct meta_extractor;
struct metrics_metadata;
struct model;
struct negation;
struct node_state;
struct offset;
struct operator_metric;
struct partition_info;
struct partition_synopsis_pair;
struct partition_synopsis;
struct passive_partition_state;
struct performance_report;
struct performance_sample;
struct predicate;
struct qualified_record_field;
struct query_context;
struct query_cursor;
struct query_status;
struct report;
struct resource;
struct rest_endpoint;
struct rest_response;
struct schema_statistics;
struct spawn_arguments;
struct status;
struct taxonomies;
struct type_extractor;
template <class Type>
struct basic_series;
using series = basic_series<type>;

enum class api_version : uint8_t;
enum class arithmetic_operator : uint8_t;
enum class bool_operator : uint8_t;
enum class ec : uint8_t;
enum class http_content_type : uint16_t;
enum class http_method : uint8_t;
enum class http_status_code : uint16_t;
enum class keep_original_partition : bool;
enum class port_type : uint8_t;
enum class query_options : uint32_t;
enum class relational_operator : uint8_t;
enum class send_initial_dbstate : bool;
enum class status_verbosity;

template <class>
class arrow_table_slice;

inline constexpr size_t dynamic_extent = std::numeric_limits<size_t>::max();

template <class>
class framed;

template <class>
class scope_linked;

template <class... Ts>
class variant;

template <typename T>
struct tag;

template <class... Ts>
class tag_variant;

namespace detail {

class legacy_deserializer;

} // namespace detail

using chunk_ptr = caf::intrusive_ptr<chunk>;
using ids = bitmap; // temporary; until we have a real type for 'ids'
using operator_ptr = std::unique_ptr<operator_base>;
using operator_type = tag_variant<void, table_slice, chunk_ptr>;
using partition_synopsis_ptr = caf::intrusive_cow_ptr<partition_synopsis>;
using value_index_ptr = std::unique_ptr<value_index>;

/// A duration in time with nanosecond resolution.
using duration = caf::timespan;

/// An absolute point in time with nanosecond resolution. It is capable to
/// represent +/- 292 years around the UNIX epoch.
using time = caf::timestamp;

/// Enumeration type.
using enumeration = uint8_t;

/// Blob type.
using blob = std::basic_string<std::byte>;

namespace fbs {

struct Bitmap;
struct Data;
struct FlatTableSlice;
struct Segment;
struct TableSlice;
struct Type;
struct TypeRegistry;
struct ValueIndex;

namespace bitmap {

struct EWAHBitmap;
struct NullBitmap;
struct WAHBitmap;

} // namespace bitmap

namespace coder {

struct SingletonCoder;
struct VectorCoder;
struct MultiLevelCoder;

} // namespace coder

namespace table_slice::arrow {

struct v2;

} // namespace table_slice::arrow

namespace value_index {

struct ArithmeticIndex;
struct EnumerationIndex;
struct HashIndex;
struct IPIndex;
struct ListIndex;
struct StringIndex;
struct SubnetIndex;

namespace detail {

struct ValueIndexBase;

} // namespace detail

} // namespace value_index

} // namespace fbs

namespace detail {

template <class Hasher>
struct hash_inspector;

struct stable_map_policy;

template <class, class, class, class>
class vector_map;

/// A map abstraction over an unsorted `std::vector`.
template <class Key, class T,
          class Allocator = std::allocator<std::pair<Key, T>>>
using stable_map = vector_map<Key, T, Allocator, stable_map_policy>;

} // namespace detail

namespace format {

class reader;
class writer;

using reader_ptr = std::unique_ptr<reader>;
using writer_ptr = std::unique_ptr<writer>;

} // namespace format

namespace ast {

struct assignment;
struct binary_expr;
struct constant;
struct dollar_var;
struct entity;
struct expression;
struct field_access;
struct function_call;
struct identifier;
struct if_stmt;
struct index_expr;
struct invocation;
struct let_stmt;
struct list;
struct match_stmt;
struct meta;
struct null;
struct pipeline_expr;
struct pipeline;
struct record;
struct selector_root;
struct unary_expr;
struct underscore;
struct unpack;

class simple_selector;

using statement
  = variant<invocation, assignment, let_stmt, if_stmt, match_stmt>;

} // namespace ast

class operator_factory_plugin;
class registry;
class session;

} // namespace tenzir

// -- type announcements -------------------------------------------------------

constexpr inline caf::type_id_t first_tenzir_type_id = 800;

CAF_BEGIN_TYPE_ID_BLOCK(tenzir_types, first_tenzir_type_id)

  TENZIR_ADD_TYPE_ID((tenzir::bitmap))
  TENZIR_ADD_TYPE_ID((tenzir::blob))
  TENZIR_ADD_TYPE_ID((tenzir::chunk_ptr))
  TENZIR_ADD_TYPE_ID((tenzir::conjunction))
  TENZIR_ADD_TYPE_ID((tenzir::curried_predicate))
  TENZIR_ADD_TYPE_ID((tenzir::data))
  TENZIR_ADD_TYPE_ID((tenzir::data_extractor))
  TENZIR_ADD_TYPE_ID((tenzir::diagnostic))
  TENZIR_ADD_TYPE_ID((tenzir::disjunction))
  TENZIR_ADD_TYPE_ID((tenzir::ec))
  TENZIR_ADD_TYPE_ID((tenzir::ewah_bitmap))
  TENZIR_ADD_TYPE_ID((tenzir::operator_metric))
  TENZIR_ADD_TYPE_ID((tenzir::expression))
  TENZIR_ADD_TYPE_ID((tenzir::extract_query_context))
  TENZIR_ADD_TYPE_ID((tenzir::field_extractor))
  TENZIR_ADD_TYPE_ID((tenzir::http_request_description))
  TENZIR_ADD_TYPE_ID((tenzir::invocation))
  TENZIR_ADD_TYPE_ID((tenzir::ip))
  TENZIR_ADD_TYPE_ID((tenzir::meta_extractor))
  TENZIR_ADD_TYPE_ID((tenzir::module))
  TENZIR_ADD_TYPE_ID((tenzir::negation))
  TENZIR_ADD_TYPE_ID((tenzir::null_bitmap))
  TENZIR_ADD_TYPE_ID((tenzir::operator_box))
  TENZIR_ADD_TYPE_ID((tenzir::operator_type))
  TENZIR_ADD_TYPE_ID((tenzir::partition_info))
  TENZIR_ADD_TYPE_ID((tenzir::partition_synopsis_pair))
  TENZIR_ADD_TYPE_ID((tenzir::partition_synopsis_ptr))
  TENZIR_ADD_TYPE_ID((tenzir::pattern))
  TENZIR_ADD_TYPE_ID((tenzir::pipeline))
  TENZIR_ADD_TYPE_ID((tenzir::port))
  TENZIR_ADD_TYPE_ID((tenzir::port_type))
  TENZIR_ADD_TYPE_ID((tenzir::predicate))
  TENZIR_ADD_TYPE_ID((tenzir::qualified_record_field))
  TENZIR_ADD_TYPE_ID((tenzir::query_context))
  TENZIR_ADD_TYPE_ID((tenzir::query_options))
  TENZIR_ADD_TYPE_ID((tenzir::relational_operator))
  TENZIR_ADD_TYPE_ID((tenzir::rest_endpoint))
  TENZIR_ADD_TYPE_ID((tenzir::rest_response))
  TENZIR_ADD_TYPE_ID((tenzir::shared_diagnostic_handler))
  TENZIR_ADD_TYPE_ID((tenzir::subnet))
  TENZIR_ADD_TYPE_ID((tenzir::table_slice))
  TENZIR_ADD_TYPE_ID((tenzir::taxonomies))
  TENZIR_ADD_TYPE_ID((tenzir::type))
  TENZIR_ADD_TYPE_ID((tenzir::type_extractor))
  TENZIR_ADD_TYPE_ID((tenzir::series))
  TENZIR_ADD_TYPE_ID((tenzir::uuid))
  TENZIR_ADD_TYPE_ID((tenzir::wah_bitmap))

  TENZIR_ADD_TYPE_ID((tenzir::tag<tenzir::table_slice>))
  TENZIR_ADD_TYPE_ID((tenzir::tag<tenzir::chunk_ptr>))

  // TODO: Make list, record, and map concrete typs to we don't need to do
  // these kinda things. See tenzir/aliases.hpp for their definitions.
  TENZIR_ADD_TYPE_ID((std::vector<tenzir::data>))
  TENZIR_ADD_TYPE_ID((tenzir::detail::stable_map<std::string, tenzir::data>))
  TENZIR_ADD_TYPE_ID((tenzir::detail::stable_map<tenzir::data, tenzir::data>))

  TENZIR_ADD_TYPE_ID((tenzir::connect_request))
  TENZIR_ADD_TYPE_ID((tenzir::metrics_metadata))
  TENZIR_ADD_TYPE_ID((tenzir::performance_report))
  TENZIR_ADD_TYPE_ID((tenzir::query_cursor))
  TENZIR_ADD_TYPE_ID((tenzir::query_status))
  TENZIR_ADD_TYPE_ID((tenzir::report))
  TENZIR_ADD_TYPE_ID((tenzir::resource))
  TENZIR_ADD_TYPE_ID((tenzir::keep_original_partition))
  TENZIR_ADD_TYPE_ID((tenzir::status_verbosity))
  TENZIR_ADD_TYPE_ID((tenzir::catalog_lookup_result))
  TENZIR_ADD_TYPE_ID((tenzir::accountant_config))
  TENZIR_ADD_TYPE_ID((tenzir::send_initial_dbstate))

  TENZIR_ADD_TYPE_ID((std::pair<std::string, tenzir::data>))
  TENZIR_ADD_TYPE_ID((std::vector<tenzir::diagnostic>))
  TENZIR_ADD_TYPE_ID((std::vector<uint32_t>))
  TENZIR_ADD_TYPE_ID((std::vector<uint64_t>))
  TENZIR_ADD_TYPE_ID((std::vector<std::string>))
  TENZIR_ADD_TYPE_ID((std::vector<tenzir::chunk_ptr>))
  TENZIR_ADD_TYPE_ID(
    (std::tuple<std::string, std::vector<tenzir::table_slice>>))
  TENZIR_ADD_TYPE_ID((std::vector<tenzir::offset>))
  TENZIR_ADD_TYPE_ID((std::vector<tenzir::partition_info>))
  TENZIR_ADD_TYPE_ID((std::vector<tenzir::series>))
  TENZIR_ADD_TYPE_ID((std::vector<std::vector<tenzir::series>>))
  TENZIR_ADD_TYPE_ID((std::vector<tenzir::table_slice>))
  TENZIR_ADD_TYPE_ID((std::vector<tenzir::uuid>))
  TENZIR_ADD_TYPE_ID(
    (std::unordered_map<tenzir::uuid, tenzir::partition_synopsis_ptr>))
  TENZIR_ADD_TYPE_ID((std::unordered_map<tenzir::type, //
                                         tenzir::catalog_lookup_result>))
  TENZIR_ADD_TYPE_ID((std::map<tenzir::uuid, tenzir::partition_synopsis_ptr>))
  TENZIR_ADD_TYPE_ID(
    (std::shared_ptr<
      std::unordered_map<tenzir::uuid, tenzir::partition_synopsis_ptr>>))
  TENZIR_ADD_TYPE_ID(
    (std::unordered_map<std::string, std::optional<std::string>>))
  TENZIR_ADD_TYPE_ID((std::vector<tenzir::partition_synopsis_pair>))

  TENZIR_ADD_TYPE_ID((caf::stream<tenzir::chunk_ptr>))
  TENZIR_ADD_TYPE_ID((caf::stream<tenzir::table_slice>))
  TENZIR_ADD_TYPE_ID((caf::inbound_stream_slot<tenzir::chunk_ptr>))
  TENZIR_ADD_TYPE_ID((caf::inbound_stream_slot<tenzir::table_slice>))
  TENZIR_ADD_TYPE_ID((caf::outbound_stream_slot<tenzir::table_slice>))

CAF_END_TYPE_ID_BLOCK(tenzir_types)

#undef TENZIR_CAF_ATOM_ALIAS
#undef TENZIR_ADD_ATOM
#undef TENZIR_ADD_TYPE_ID
