//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/config.hpp" // IWYU pragma: export

#include <caf/config.hpp>
#include <caf/fwd.hpp>
#include <caf/type_id.hpp>
#include <flatbuffers/base.h>

#include <cstdint>
#include <filesystem>
#include <map>
#include <unordered_map>
#include <vector>

#define VAST_ADD_TYPE_ID(type) CAF_ADD_TYPE_ID(vast_types, type)

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
class FieldPath;
class Int64Type;
class ListType;
class MapType;
class MemoryPool;
class RecordBatch;
class Schema;
class StringType;
class StructType;
class TimestampType;
class UInt64Type;

namespace io {

class RandomAccessFile;

} // namespace io

} // namespace arrow

// -- flatbuffers -------------------------------------------------------------

namespace flatbuffers {

#if FLATBUFFERS_VERSION_MAJOR >= 23 && FLATBUFFERS_VERSION_MINOR >= 5          \
  && FLATBUFFERS_VERSION_REVISION >= 9

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

// -- vast ---------------------------------------------------------------------

namespace vast {

class active_store;
class aggregation_function;
class bitmap;
class bool_type;
class chunk;
class command;
class data;
class double_type;
class duration_type;
class enumeration_type;
class ewah_bitmap;
class expression;
class http_request;
class http_request_description;
class int64_type;
class ip_type;
class ip;
class legacy_abstract_type;
class legacy_type;
class list_type;
class map_type;
class module;
class null_bitmap;
class operator_base;
class passive_store;
class pattern;
class pipeline;
class plugin_ptr;
class plugin;
class port;
class record_type;
class segment;
class string_type;
class subnet_type;
class subnet;
class synopsis;
class table_slice_builder;
class table_slice_column;
class table_slice;
class time_type;
class type;
class uint64_type;
class uuid;
class value_index;
class wah_bitmap;
class pipeline;

struct attribute;
struct concept_;
struct conjunction;
struct count_query_context;
struct curried_predicate;
struct data_extractor;
struct disjunction;
struct extract_query_context;
struct field_extractor;
struct flow;
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
struct meta_extractor;
struct model;
struct negation;
struct offset;
struct partition_info;
struct partition_synopsis_pair;
struct partition_synopsis;
struct predicate;
struct qualified_record_field;
struct query_context;
struct rest_endpoint;
struct schema_statistics;
struct status;
struct taxonomies;
struct type_extractor;
struct type_set;

enum class api_version : uint8_t;
enum class arithmetic_operator : uint8_t;
enum class bool_operator : uint8_t;
enum class ec : uint8_t;
enum class http_content_type : uint16_t;
enum class http_method : uint8_t;
enum class http_status_code : uint16_t;
enum class port_type : uint8_t;
enum class query_options : uint32_t;
enum class relational_operator : uint8_t;
enum class table_slice_encoding : uint8_t;

template <class>
class arrow_table_slice;

inline constexpr size_t dynamic_extent = std::numeric_limits<size_t>::max();

template <class>
class framed;

template <class... Types>
class projection;

template <class>
class scope_linked;

namespace detail {

class legacy_deserializer;

} // namespace detail

using chunk_ptr = caf::intrusive_ptr<chunk>;
using ids = bitmap; // temporary; until we have a real type for 'ids'
using partition_synopsis_ptr = caf::intrusive_cow_ptr<partition_synopsis>;
using value_index_ptr = std::unique_ptr<value_index>;

/// A duration in time with nanosecond resolution.
using duration = caf::timespan;

/// An absolute point in time with nanosecond resolution. It is capable to
/// represent +/- 292 years around the UNIX epoch.
using time = caf::timestamp;

/// Enumeration type.
using enumeration = uint8_t;

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

namespace system {

class configuration;

struct accountant_config;
struct active_partition_state;
struct component_map;
struct component_map_entry;
struct component_state;
struct component_state_map;
struct connect_request;
struct data_point;
struct index_state;
struct measurement;
struct catalog_lookup_result;
struct metrics_metadata;
struct node_state;
struct passive_partition_state;
struct performance_report;
struct performance_sample;
struct query_cursor;
struct query_status;
struct report;
struct spawn_arguments;

enum class keep_original_partition : bool;
enum class send_initial_dbstate : bool;
enum class status_verbosity;

} // namespace system

} // namespace vast

// -- type announcements -------------------------------------------------------

constexpr inline caf::type_id_t first_vast_type_id = 800;

CAF_BEGIN_TYPE_ID_BLOCK(vast_types, first_vast_type_id)

  VAST_ADD_TYPE_ID((vast::bitmap))
  VAST_ADD_TYPE_ID((vast::chunk_ptr))
  VAST_ADD_TYPE_ID((vast::conjunction))
  VAST_ADD_TYPE_ID((vast::count_query_context))
  VAST_ADD_TYPE_ID((vast::curried_predicate))
  VAST_ADD_TYPE_ID((vast::data_extractor))
  VAST_ADD_TYPE_ID((vast::data))
  VAST_ADD_TYPE_ID((vast::disjunction))
  VAST_ADD_TYPE_ID((vast::ec))
  VAST_ADD_TYPE_ID((vast::ewah_bitmap))
  VAST_ADD_TYPE_ID((vast::expression))
  VAST_ADD_TYPE_ID((vast::extract_query_context))
  VAST_ADD_TYPE_ID((vast::field_extractor))
  VAST_ADD_TYPE_ID((vast::http_request))
  VAST_ADD_TYPE_ID((vast::http_request_description))
  VAST_ADD_TYPE_ID((vast::invocation))
  VAST_ADD_TYPE_ID((vast::ip))
  VAST_ADD_TYPE_ID((vast::meta_extractor))
  VAST_ADD_TYPE_ID((vast::module))
  VAST_ADD_TYPE_ID((vast::negation))
  VAST_ADD_TYPE_ID((vast::null_bitmap))
  VAST_ADD_TYPE_ID((vast::partition_info))
  VAST_ADD_TYPE_ID((vast::partition_synopsis_pair))
  VAST_ADD_TYPE_ID((vast::partition_synopsis_ptr))
  VAST_ADD_TYPE_ID((vast::pattern))
  VAST_ADD_TYPE_ID((vast::pipeline))
  VAST_ADD_TYPE_ID((vast::port_type))
  VAST_ADD_TYPE_ID((vast::port))
  VAST_ADD_TYPE_ID((vast::predicate))
  VAST_ADD_TYPE_ID((vast::qualified_record_field))
  VAST_ADD_TYPE_ID((vast::query_context))
  VAST_ADD_TYPE_ID((vast::query_options))
  VAST_ADD_TYPE_ID((vast::relational_operator))
  VAST_ADD_TYPE_ID((vast::rest_endpoint))
  VAST_ADD_TYPE_ID((vast::subnet))
  VAST_ADD_TYPE_ID((vast::table_slice_column))
  VAST_ADD_TYPE_ID((vast::table_slice))
  VAST_ADD_TYPE_ID((vast::taxonomies))
  VAST_ADD_TYPE_ID((vast::type_extractor))
  VAST_ADD_TYPE_ID((vast::type_set))
  VAST_ADD_TYPE_ID((vast::type))
  VAST_ADD_TYPE_ID((vast::uuid))
  VAST_ADD_TYPE_ID((vast::wah_bitmap))

  // TODO: Make list, record, and map concrete typs to we don't need to do
  // these kinda things. See vast/aliases.hpp for their definitions.
  VAST_ADD_TYPE_ID((std::vector<vast::data>))
  VAST_ADD_TYPE_ID((vast::detail::stable_map<std::string, vast::data>))
  VAST_ADD_TYPE_ID((vast::detail::stable_map<vast::data, vast::data>))

  VAST_ADD_TYPE_ID((vast::system::connect_request))
  VAST_ADD_TYPE_ID((vast::system::metrics_metadata))
  VAST_ADD_TYPE_ID((vast::system::performance_report))
  VAST_ADD_TYPE_ID((vast::system::query_cursor))
  VAST_ADD_TYPE_ID((vast::system::query_status))
  VAST_ADD_TYPE_ID((vast::system::report))
  VAST_ADD_TYPE_ID((vast::system::keep_original_partition))
  VAST_ADD_TYPE_ID((vast::system::status_verbosity))
  VAST_ADD_TYPE_ID((vast::system::catalog_lookup_result))
  VAST_ADD_TYPE_ID((vast::system::accountant_config))
  VAST_ADD_TYPE_ID((vast::system::send_initial_dbstate))

  VAST_ADD_TYPE_ID((vast::framed<vast::chunk_ptr>))
  VAST_ADD_TYPE_ID((vast::framed<vast::table_slice>))
  VAST_ADD_TYPE_ID((std::pair<std::string, vast::data>))
  VAST_ADD_TYPE_ID((std::vector<uint32_t>))
  VAST_ADD_TYPE_ID((std::vector<uint64_t>))
  VAST_ADD_TYPE_ID((std::vector<std::string>))
  VAST_ADD_TYPE_ID((std::vector<vast::chunk_ptr>))
  VAST_ADD_TYPE_ID((std::vector<vast::table_slice>))
  VAST_ADD_TYPE_ID((std::vector<vast::framed<vast::chunk_ptr>>))
  VAST_ADD_TYPE_ID((std::vector<vast::framed<vast::table_slice>>))
  VAST_ADD_TYPE_ID((std::vector<vast::table_slice_column>))
  VAST_ADD_TYPE_ID((std::vector<vast::uuid>))
  VAST_ADD_TYPE_ID((std::vector<vast::partition_info>))
  VAST_ADD_TYPE_ID(
    (std::unordered_map<vast::uuid, vast::partition_synopsis_ptr>))
  VAST_ADD_TYPE_ID((std::unordered_map<vast::type, //
                                       vast::system::catalog_lookup_result>))
  VAST_ADD_TYPE_ID((std::map<vast::uuid, vast::partition_synopsis_ptr>))
  VAST_ADD_TYPE_ID(
    (std::shared_ptr<
      std::unordered_map<vast::uuid, vast::partition_synopsis_ptr>>))
  VAST_ADD_TYPE_ID((std::vector<vast::partition_synopsis_pair>))

  VAST_ADD_TYPE_ID((caf::stream<vast::chunk_ptr>))
  VAST_ADD_TYPE_ID((caf::stream<vast::table_slice>))
  VAST_ADD_TYPE_ID((caf::stream<vast::framed<vast::chunk_ptr>>))
  VAST_ADD_TYPE_ID((caf::stream<vast::framed<vast::table_slice>>))
  VAST_ADD_TYPE_ID((caf::stream<vast::table_slice_column>))
  VAST_ADD_TYPE_ID((caf::inbound_stream_slot<vast::chunk_ptr>))
  VAST_ADD_TYPE_ID((caf::inbound_stream_slot<vast::table_slice>))
  VAST_ADD_TYPE_ID((caf::inbound_stream_slot<vast::framed<vast::chunk_ptr>>))
  VAST_ADD_TYPE_ID((caf::inbound_stream_slot<vast::framed<vast::table_slice>>))
  VAST_ADD_TYPE_ID((caf::inbound_stream_slot<vast::table_slice_column>))
  VAST_ADD_TYPE_ID((caf::outbound_stream_slot<vast::table_slice>))

CAF_END_TYPE_ID_BLOCK(vast_types)

#undef VAST_CAF_ATOM_ALIAS
#undef VAST_ADD_ATOM
#undef VAST_ADD_TYPE_ID
