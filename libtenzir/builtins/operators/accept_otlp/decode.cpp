//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/as_bytes.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/concept/parseable/tenzir/ip.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/base64.hpp"
#include "tenzir/detail/coding.hpp"
#include "tenzir/detail/escapers.hpp"
#include "tenzir/detail/hex_encode.hpp"
#include "tenzir/detail/string.hpp"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/util/json_util.h>

#include <cctype>
#include <limits>
#include <simdjson.h>
#include <span>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "decode_internal.hpp"

namespace tenzir::plugins::accept_otlp::detail {

auto decode_hex_id(std::string_view value) -> Result<std::string, std::string> {
  if (value.empty()) {
    return std::string{};
  }
  if (value.size() % 2 != 0) {
    return Err{std::string{"ID contains an odd number of hexadecimal digits"}};
  }
  auto result = tenzir::detail::hex::decode(value);
  if (not result) {
    return Err{std::string{"ID contains a non-hexadecimal character"}};
  }
  return std::move(*result);
}

enum class InvalidJsonIdPolicy {
  reject,
  clear,
};

auto find_json_string_end(std::string_view json, size_t begin)
  -> Result<size_t, std::string> {
  TENZIR_ASSERT(begin < json.size() and json[begin] == '"');
  auto escaped = false;
  for (auto i = begin + 1; i < json.size(); ++i) {
    if (escaped) {
      escaped = false;
      continue;
    }
    if (json[i] == '\\') {
      escaped = true;
      continue;
    }
    if (json[i] == '"') {
      return i + 1;
    }
  }
  return Err{std::string{"unterminated JSON string"}};
}

auto find_json_value_end(std::string_view json, size_t begin)
  -> Result<size_t, std::string> {
  if (begin >= json.size()) {
    return Err{std::string{"missing JSON value"}};
  }
  if (json[begin] == '"') {
    return find_json_string_end(json, begin);
  }
  if (json[begin] == '{' or json[begin] == '[') {
    auto depth = size_t{0};
    auto i = begin;
    while (i < json.size()) {
      if (json[i] == '"') {
        auto end = find_json_string_end(json, i);
        if (end.is_err()) {
          return Err{std::move(end).unwrap_err()};
        }
        i = std::move(end).unwrap();
        continue;
      }
      if (json[i] == '{' or json[i] == '[') {
        ++depth;
      } else if (json[i] == '}' or json[i] == ']') {
        --depth;
        if (depth == 0) {
          return i + 1;
        }
      }
      ++i;
    }
    return Err{std::string{"unterminated JSON value"}};
  }
  auto i = begin;
  while (i < json.size() and json[i] != ',' and json[i] != ']'
         and json[i] != '}'
         and not std::isspace(static_cast<unsigned char>(json[i]))) {
    ++i;
  }
  if (i == begin) {
    return Err{std::string{"missing JSON value"}};
  }
  return i;
}

auto find_json_field(google::protobuf::Descriptor const& descriptor,
                     std::string_view name)
  -> google::protobuf::FieldDescriptor const* {
  auto const owned_name = std::string{name};
  if (auto const* field = descriptor.FindFieldByName(owned_name)) {
    return field;
  }
  if (auto const* field = descriptor.FindFieldByCamelcaseName(owned_name)) {
    return field;
  }
  for (auto index = 0; index < descriptor.field_count(); ++index) {
    auto const* field = descriptor.field(index);
    if (field->json_name() == name) {
      return field;
    }
  }
  return nullptr;
}

auto is_otlp_id_field(google::protobuf::FieldDescriptor const& field) -> bool {
  if (field.type() != google::protobuf::FieldDescriptor::TYPE_BYTES) {
    return false;
  }
  auto const name = field.name();
  return name == "trace_id" or name == "span_id" or name == "parent_span_id";
}

auto normalize_otlp_json_id(std::string_view value, std::string_view key,
                            InvalidJsonIdPolicy invalid_id_policy)
  -> Result<std::string, std::string> {
  if (value.empty() or value.front() != '"') {
    if (invalid_id_policy == InvalidJsonIdPolicy::reject) {
      return Err{fmt::format("OTLP/JSON field `{}` must be a string", key)};
    }
    return std::string{"\"\""};
  }
  auto unescaped = tenzir::detail::json_unescape(value);
  auto decoded = decode_hex_id(unescaped);
  if (decoded.is_err()) {
    if (invalid_id_policy == InvalidJsonIdPolicy::reject) {
      return Err{
        fmt::format("invalid `{}`: {}", key, std::move(decoded).unwrap_err())};
    }
    return std::string{"\"\""};
  }
  return fmt::format(
    "\"{}\"", tenzir::detail::base64::encode(std::move(decoded).unwrap()));
}

using DescriptorIdCache
  = std::unordered_map<google::protobuf::Descriptor const*, bool>;

auto descriptor_contains_otlp_id(google::protobuf::Descriptor const& descriptor,
                                 DescriptorIdCache& cache) -> bool {
  if (auto const it = cache.find(&descriptor); it != cache.end()) {
    return it->second;
  }
  // Insert a provisional result before descending to break recursive message
  // cycles such as AnyValue -> ArrayValue -> AnyValue.
  auto [entry, inserted] = cache.emplace(&descriptor, false);
  TENZIR_ASSERT(inserted);
  for (auto index = 0; index < descriptor.field_count(); ++index) {
    auto const& field = *descriptor.field(index);
    if (is_otlp_id_field(field)
        or (field.cpp_type()
              == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE
            and descriptor_contains_otlp_id(*field.message_type(), cache))) {
      entry->second = true;
      break;
    }
  }
  return entry->second;
}

auto normalize_otlp_json_object(std::string_view json,
                                google::protobuf::Descriptor const& descriptor,
                                InvalidJsonIdPolicy invalid_id_policy,
                                DescriptorIdCache& cache)
  -> Result<std::string, std::string>;

auto normalize_otlp_json_array(std::string_view json,
                               google::protobuf::Descriptor const& descriptor,
                               InvalidJsonIdPolicy invalid_id_policy,
                               DescriptorIdCache& cache)
  -> Result<std::string, std::string> {
  auto result = std::string{};
  result.reserve(json.size());
  auto i = size_t{0};
  while (i < json.size() and json[i] != '[') {
    result.push_back(json[i++]);
  }
  if (i == json.size()) {
    return std::string{json};
  }
  result.push_back(json[i++]);
  while (i < json.size()) {
    while (i < json.size()
           and (json[i] == ','
                or std::isspace(static_cast<unsigned char>(json[i])))) {
      result.push_back(json[i++]);
    }
    if (i == json.size() or json[i] == ']') {
      result.append(json.substr(i));
      return result;
    }
    TRY(auto end, find_json_value_end(json, i));
    auto const value = json.substr(i, end - i);
    if (value.front() == '{') {
      TRY(auto normalized, normalize_otlp_json_object(
                             value, descriptor, invalid_id_policy, cache));
      result.append(normalized);
    } else {
      result.append(value);
    }
    i = end;
  }
  return result;
}

auto normalize_otlp_json_object(std::string_view json,
                                google::protobuf::Descriptor const& descriptor,
                                InvalidJsonIdPolicy invalid_id_policy,
                                DescriptorIdCache& cache)
  -> Result<std::string, std::string> {
  auto result = std::string{};
  result.reserve(json.size());
  auto i = size_t{0};
  while (i < json.size() and json[i] != '{') {
    result.push_back(json[i++]);
  }
  if (i == json.size()) {
    return std::string{json};
  }
  result.push_back(json[i++]);
  while (i < json.size()) {
    while (i < json.size() and json[i] != '"' and json[i] != '}') {
      result.push_back(json[i++]);
    }
    if (i == json.size() or json[i] == '}') {
      result.append(json.substr(i));
      return result;
    }
    auto const key_begin = i;
    TRY(auto key_end, find_json_string_end(json, key_begin));
    auto const key_token = json.substr(key_begin, key_end - key_begin);
    auto const key = tenzir::detail::json_unescape(key_token);
    result.append(key_token);
    i = key_end;
    while (i < json.size() and json[i] != ':') {
      result.push_back(json[i++]);
    }
    if (i == json.size()) {
      return result;
    }
    result.push_back(json[i++]);
    while (i < json.size()
           and std::isspace(static_cast<unsigned char>(json[i]))) {
      result.push_back(json[i++]);
    }
    TRY(auto value_end, find_json_value_end(json, i));
    auto const value = json.substr(i, value_end - i);
    auto const* field = find_json_field(descriptor, key);
    if (field and is_otlp_id_field(*field)) {
      TRY(auto normalized,
          normalize_otlp_json_id(value, key, invalid_id_policy));
      result.append(normalized);
    } else if (field
               and field->cpp_type()
                     == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
      auto const& nested = *field->message_type();
      if (not descriptor_contains_otlp_id(nested, cache)) {
        result.append(value);
      } else if (field->is_repeated() and value.front() == '[') {
        TRY(auto normalized,
            normalize_otlp_json_array(value, nested, invalid_id_policy, cache));
        result.append(normalized);
      } else if (not field->is_repeated() and value.front() == '{') {
        TRY(auto normalized, normalize_otlp_json_object(
                               value, nested, invalid_id_policy, cache));
        result.append(normalized);
      } else {
        result.append(value);
      }
    } else {
      result.append(value);
    }
    i = value_end;
  }
  return result;
}

/// Rewrites descriptor-known OTLP/JSON ID fields from hexadecimal to the
/// ProtoJSON base64 representation. Unknown fields remain byte-for-byte
/// unchanged so that Protobuf can ignore them for forward compatibility.
auto normalize_otlp_json_ids(std::string_view json,
                             google::protobuf::Descriptor const& descriptor,
                             InvalidJsonIdPolicy invalid_id_policy)
  -> Result<std::string, std::string> {
  auto padded = simdjson::padded_string{json};
  auto parser = simdjson::dom::parser{};
  if (parser.parse(padded).error() != simdjson::SUCCESS) {
    return Err{std::string{"failed to decode OTLP/JSON"}};
  }
  auto cache = DescriptorIdCache{};
  return normalize_otlp_json_object(json, descriptor, invalid_id_policy, cache);
}

template <class Options>
auto configure_json_options(Options& options) -> void {
  if constexpr (requires { options.allow_legacy_nonconformant_behavior; }) {
    options.allow_legacy_nonconformant_behavior = false;
  }
}

template <class Message>
auto parse_message(std::span<std::byte const> bytes, Encoding encoding,
                   InvalidJsonIdPolicy invalid_id_policy
                   = InvalidJsonIdPolicy::reject)
  -> Result<Message, std::string> {
  auto message = Message{};
  if (encoding == Encoding::protobuf) {
    if (not message.ParseFromArray(bytes.data(),
                                   tenzir::detail::narrow<int>(bytes.size()))) {
      return Err{std::string{"failed to decode binary Protobuf"}};
    }
    return message;
  }
  auto json = std::string_view{reinterpret_cast<char const*>(bytes.data()),
                               bytes.size()};
  auto normalized = normalize_otlp_json_ids(json, *message.GetDescriptor(),
                                            invalid_id_policy);
  if (normalized.is_err()) {
    return Err{std::move(normalized).unwrap_err()};
  }
  auto options = google::protobuf::util::JsonParseOptions{};
  configure_json_options(options);
  options.ignore_unknown_fields = true;
  auto status = google::protobuf::util::JsonStringToMessage(
    std::move(normalized).unwrap(), &message, options);
  if (not status.ok()) {
    return Err{std::string{"failed to decode OTLP/JSON"}};
  }
  return message;
}

auto bytes_data(std::string_view bytes) -> data {
  return blob{as_bytes(bytes)};
}

auto id_string(std::string_view bytes) -> data {
  if (bytes.empty()) {
    return {};
  }
  return tenzir::detail::hexify(as_bytes(bytes));
}

auto timestamp(uint64_t nanos) -> data {
  if (nanos == 0) {
    return {};
  }
  return time{std::chrono::nanoseconds{tenzir::detail::narrow<int64_t>(nanos)}};
}

auto nullable_string(std::string const& value) -> data {
  return value.empty() ? data{} : data{value};
}

auto canonical_json(common::AnyValue const& value) -> std::string {
  auto result = std::string{};
  auto options = google::protobuf::util::JsonPrintOptions{};
  configure_json_options(options);
  options.always_print_enums_as_ints = true;
  auto status
    = google::protobuf::util::MessageToJsonString(value, &result, options);
  TENZIR_ASSERT(status.ok());
  return result;
}

auto any_value_kind(common::AnyValue const& value) -> std::string_view {
  switch (value.value_case()) {
    case common::AnyValue::kStringValue:
      return "string";
    case common::AnyValue::kBoolValue:
      return "bool";
    case common::AnyValue::kIntValue:
      return "int";
    case common::AnyValue::kDoubleValue:
      return "double";
    case common::AnyValue::kArrayValue:
      return "array";
    case common::AnyValue::kKvlistValue:
      return "kvlist";
    case common::AnyValue::kBytesValue:
      return "bytes";
    case common::AnyValue::VALUE_NOT_SET:
      return "empty";
  }
  TENZIR_UNREACHABLE();
}

auto make_tagged_any_value(common::AnyValue const& value,
                           DecodeContext const& ctx)
  -> Result<data, std::string> {
  if (ctx.is_cancelled()) {
    return Err{std::string{cancelled_error}};
  }
  auto result = record{
    {"kind", std::string{any_value_kind(value)}},
    {"string_value", data{}},
    {"bool_value", data{}},
    {"int_value", data{}},
    {"double_value", data{}},
    {"bytes_value", data{}},
    {"json_value", data{}},
  };
  switch (value.value_case()) {
    case common::AnyValue::kStringValue:
      result["string_value"] = value.string_value();
      break;
    case common::AnyValue::kBoolValue:
      result["bool_value"] = value.bool_value();
      break;
    case common::AnyValue::kIntValue:
      result["int_value"] = value.int_value();
      break;
    case common::AnyValue::kDoubleValue:
      result["double_value"] = value.double_value();
      break;
    case common::AnyValue::kBytesValue:
      result["bytes_value"] = bytes_data(value.bytes_value());
      break;
    case common::AnyValue::kArrayValue:
    case common::AnyValue::kKvlistValue:
      result["json_value"] = canonical_json(value);
      break;
    case common::AnyValue::VALUE_NOT_SET:
      break;
  }
  if (ctx.is_cancelled()) {
    return Err{std::string{cancelled_error}};
  }
  return data{std::move(result)};
}

auto make_native_any_value(common::AnyValue const& value,
                           DecodeContext const& ctx)
  -> Result<data, std::string> {
  if (ctx.is_cancelled()) {
    return Err{std::string{cancelled_error}};
  }
  auto result = data{};
  switch (value.value_case()) {
    case common::AnyValue::kStringValue:
      result = value.string_value();
      break;
    case common::AnyValue::kBoolValue:
      result = value.bool_value();
      break;
    case common::AnyValue::kIntValue:
      result = value.int_value();
      break;
    case common::AnyValue::kDoubleValue:
      result = value.double_value();
      break;
    case common::AnyValue::kBytesValue:
      result = bytes_data(value.bytes_value());
      break;
    case common::AnyValue::kArrayValue:
    case common::AnyValue::kKvlistValue:
      result = canonical_json(value);
      break;
    case common::AnyValue::VALUE_NOT_SET:
      break;
  }
  if (ctx.is_cancelled()) {
    return Err{std::string{cancelled_error}};
  }
  return result;
}

auto make_receiver(DecodeContext const& ctx) -> data {
  auto const transport = ctx.transport == Transport::http ? "http" : "grpc";
  return record{{"transport", transport},
                {"peer_ip", ctx.peer_ip},
                {"metadata", ctx.metadata}};
}

auto make_entity_refs(resource::Resource const& value, DecodeContext const& ctx)
  -> Result<list, std::string> {
  auto result = list{};
  result.reserve(tenzir::detail::narrow<size_t>(value.entity_refs_size()));
  for (auto const& entity_ref : value.entity_refs()) {
    if (ctx.is_cancelled()) {
      return Err{std::string{cancelled_error}};
    }
    auto id_keys = list{};
    id_keys.reserve(tenzir::detail::narrow<size_t>(entity_ref.id_keys_size()));
    for (auto const& key : entity_ref.id_keys()) {
      if (ctx.is_cancelled()) {
        return Err{std::string{cancelled_error}};
      }
      id_keys.emplace_back(key);
    }
    auto description_keys = list{};
    description_keys.reserve(
      tenzir::detail::narrow<size_t>(entity_ref.description_keys_size()));
    for (auto const& key : entity_ref.description_keys()) {
      if (ctx.is_cancelled()) {
        return Err{std::string{cancelled_error}};
      }
      description_keys.emplace_back(key);
    }
    result.emplace_back(record{
      {"schema_url", nullable_string(entity_ref.schema_url())},
      {"type", entity_ref.type()},
      {"id_keys", std::move(id_keys)},
      {"description_keys", std::move(description_keys)},
    });
  }
  return result;
}

auto make_resource(resource::Resource const& value,
                   std::string const& schema_url, DecodeContext const& ctx)
  -> Result<data, std::string> {
  TRY(auto attributes, make_attributes(value.attributes(), ctx));
  TRY(auto entity_refs, make_entity_refs(value, ctx));
  return data{record{
    {"attributes", std::move(attributes)},
    {"dropped_attributes_count",
     static_cast<uint64_t>(value.dropped_attributes_count())},
    {"entity_refs", std::move(entity_refs)},
    {"schema_url", nullable_string(schema_url)},
  }};
}

auto make_scope(common::InstrumentationScope const& value,
                std::string const& schema_url, DecodeContext const& ctx)
  -> Result<data, std::string> {
  TRY(auto attributes, make_attributes(value.attributes(), ctx));
  return data{record{
    {"name", nullable_string(value.name())},
    {"version", nullable_string(value.version())},
    {"attributes", std::move(attributes)},
    {"dropped_attributes_count",
     static_cast<uint64_t>(value.dropped_attributes_count())},
    {"schema_url", nullable_string(schema_url)},
  }};
}

auto with_context(resource::Resource const& resource_value,
                  std::string const& resource_schema_url,
                  common::InstrumentationScope const& scope_value,
                  std::string const& scope_schema_url, DecodeContext const& ctx)
  -> Result<record, std::string> {
  TRY(auto resource, make_resource(resource_value, resource_schema_url, ctx));
  TRY(auto scope, make_scope(scope_value, scope_schema_url, ctx));
  if (ctx.is_cancelled()) {
    return Err{std::string{cancelled_error}};
  }
  return record{{"resource", std::move(resource)},
                {"scope", std::move(scope)},
                {"receiver", make_receiver(ctx)}};
}

// Keep repeated resource, scope, metric, and receiver context near 1 MiB per
// slice. Non-repeated fields remain bounded by the request message-size limit.
auto batch_row_limit(resource::Resource const& resource_value,
                     std::string const& resource_schema_url,
                     common::InstrumentationScope const& scope_value,
                     std::string const& scope_schema_url,
                     DecodeContext const& ctx, size_t additional_size)
  -> int64_t {
  constexpr auto target_batch_size = uint64_t{1_Mi};
  auto repeated_size = uint64_t{resource_value.ByteSizeLong()}
                       + resource_schema_url.size() + scope_value.ByteSizeLong()
                       + scope_schema_url.size() + ctx.receiver_metadata_size
                       + additional_size;
  repeated_size = std::max(repeated_size, uint64_t{1});
  auto const rows = std::clamp(target_batch_size / repeated_size, uint64_t{1},
                               uint64_t{defaults::import::table_slice_size});
  return tenzir::detail::narrow<int64_t>(rows);
}

auto attributes_type(AttributeMode mode) -> type {
  if (mode == AttributeMode::record) {
    return type{record_type{}};
  }
  auto const any_value = record_type{
    {"kind", string_type{}},         {"string_value", string_type{}},
    {"bool_value", bool_type{}},     {"int_value", int64_type{}},
    {"double_value", double_type{}}, {"bytes_value", blob_type{}},
    {"json_value", string_type{}},
  };
  return type{
    list_type{record_type{{"key", string_type{}}, {"value", any_value}}}};
}

auto common_fields(AttributeMode mode)
  -> std::vector<struct record_type::field> {
  auto const attrs = attributes_type(mode);
  auto const entity_ref = record_type{
    {"schema_url", string_type{}},
    {"type", string_type{}},
    {"id_keys", list_type{string_type{}}},
    {"description_keys", list_type{string_type{}}},
  };
  return {
    {"resource", record_type{{"attributes", attrs},
                             {"dropped_attributes_count", uint64_type{}},
                             {"entity_refs", list_type{entity_ref}},
                             {"schema_url", string_type{}}}},
    {"scope", record_type{{"name", string_type{}},
                          {"version", string_type{}},
                          {"attributes", attrs},
                          {"dropped_attributes_count", uint64_type{}},
                          {"schema_url", string_type{}}}},
    {"receiver", record_type{{"transport", string_type{}},
                             {"peer_ip", ip_type{}},
                             {"metadata", list_type{record_type{
                                            {"name", string_type{}},
                                            {"value", string_type{}},
                                          }}}}},
  };
}

auto append_fields(std::vector<struct record_type::field> fields,
                   std::initializer_list<record_type::field_view> extra)
  -> record_type {
  for (auto const& field : extra) {
    fields.emplace_back(std::string{field.name}, field.type);
  }
  return record_type{fields};
}

auto has_nonzero_byte(std::string_view id) -> bool {
  return std::ranges::any_of(id, [](char c) {
    return c != '\0';
  });
}

auto optional_id_string(std::string_view id, size_t size) -> data {
  if (id.size() != size or not has_nonzero_byte(id)) {
    return {};
  }
  return id_string(id);
}

auto validate_id(std::string_view id, size_t size, bool required,
                 std::string_view field) -> Result<Empty, std::string> {
  if (id.empty() and not required) {
    return Empty{};
  }
  if (id.size() != size or not has_nonzero_byte(id)) {
    return Err{fmt::format("`{}` must be a non-zero {}-byte ID", field, size)};
  }
  return Empty{};
}

auto validate_time(uint64_t value, bool required, std::string_view field)
  -> Result<Empty, std::string> {
  if (value == 0 and not required) {
    return Empty{};
  }
  if (value == 0) {
    return Err{fmt::format("`{}` must not be zero", field)};
  }
  if (value > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    return Err{fmt::format("`{}` exceeds Tenzir's timestamp range", field)};
  }
  return Empty{};
}

auto validate_any_value(common::AnyValue const& value, DecodeContext const* ctx)
  -> Result<Empty, std::string> {
  if (ctx and ctx->is_cancelled()) {
    return Err{std::string{cancelled_error}};
  }
  if (value.has_array_value()) {
    for (auto const& nested : value.array_value().values()) {
      auto valid = validate_any_value(nested, ctx);
      if (valid.is_err()) {
        return valid;
      }
    }
  }
  if (value.has_kvlist_value()) {
    return validate_attributes(value.kvlist_value().values(), true, ctx);
  }
  return Empty{};
}

auto validate_resource(resource::Resource const& value, bool unique,
                       DecodeContext const* ctx) -> Result<Empty, std::string> {
  auto result = validate_attributes(value.attributes(), unique, ctx);
  if (result.is_err()) {
    return result;
  }
  auto attribute_keys = std::unordered_set<std::string_view>{};
  for (auto const& attribute : value.attributes()) {
    if (ctx and ctx->is_cancelled()) {
      return Err{std::string{cancelled_error}};
    }
    attribute_keys.emplace(attribute.key());
  }
  for (auto const& entity_ref : value.entity_refs()) {
    if (ctx and ctx->is_cancelled()) {
      return Err{std::string{cancelled_error}};
    }
    if (entity_ref.type().empty()) {
      return Err{std::string{"entity reference type must not be empty"}};
    }
    if (entity_ref.id_keys().empty()) {
      return Err{
        std::string{"entity reference must contain at least one ID key"}};
    }
    for (auto const& key : entity_ref.id_keys()) {
      if (ctx and ctx->is_cancelled()) {
        return Err{std::string{cancelled_error}};
      }
      if (not attribute_keys.contains(key)) {
        return Err{fmt::format("entity reference ID key `{}` does not name a "
                               "resource attribute",
                               key)};
      }
    }
    for (auto const& key : entity_ref.description_keys()) {
      if (ctx and ctx->is_cancelled()) {
        return Err{std::string{cancelled_error}};
      }
      if (not attribute_keys.contains(key)) {
        return Err{fmt::format("entity reference description key `{}` does not "
                               "name a resource attribute",
                               key)};
      }
    }
  }
  return Empty{};
}

auto decode(Signal signal, Encoding encoding, std::span<std::byte const> bytes,
            DecodeContext ctx) -> DecodeResult {
  switch (signal) {
    case Signal::logs: {
      auto request = parse_message<collector_logs::ExportLogsServiceRequest>(
        bytes, encoding, InvalidJsonIdPolicy::clear);
      if (request.is_err()) {
        return Err{std::move(request).unwrap_err()};
      }
      return decode_logs(std::move(request).unwrap(), std::move(ctx));
    }
    case Signal::metrics: {
      auto request
        = parse_message<collector_metrics::ExportMetricsServiceRequest>(
          bytes, encoding);
      if (request.is_err()) {
        return Err{std::move(request).unwrap_err()};
      }
      return decode_metrics(std::move(request).unwrap(), std::move(ctx));
    }
    case Signal::traces: {
      auto request = parse_message<collector_trace::ExportTraceServiceRequest>(
        bytes, encoding);
      if (request.is_err()) {
        return Err{std::move(request).unwrap_err()};
      }
      return decode_traces(std::move(request).unwrap(), std::move(ctx));
    }
  }
  TENZIR_UNREACHABLE();
}

auto decode(GrpcRequest request, DecodeContext ctx) -> DecodeResult {
  return match(std::move(request), [&](auto&& typed_request) -> DecodeResult {
    using Request = std::remove_cvref_t<decltype(typed_request)>;
    if constexpr (std::same_as<Request,
                               collector_logs::ExportLogsServiceRequest>) {
      return decode_logs(std::move(typed_request), std::move(ctx));
    } else if constexpr (std::same_as<
                           Request,
                           collector_metrics::ExportMetricsServiceRequest>) {
      return decode_metrics(std::move(typed_request), std::move(ctx));
    } else {
      return decode_traces(std::move(typed_request), std::move(ctx));
    }
  });
}

auto make_decode_context(RequestMetadata const& metadata,
                         AcceptOtlpArgs const& args)
  -> Result<DecodeContext, std::string> {
  auto peer_ip = ip{};
  if (not parsers::ip(metadata.client_ip, peer_ip)) {
    return Err{fmt::format("failed to parse peer IP `{}`", metadata.client_ip)};
  }
  auto included = std::unordered_set<std::string>{};
  if (args.include_metadata) {
    for (auto const& value : args.include_metadata->inner) {
      auto const* name = try_as<std::string>(&value);
      TENZIR_ASSERT(name);
      included.emplace(tenzir::detail::ascii_tolower(*name));
    }
  }
  auto selected = list{};
  auto selected_size = size_t{};
  for (auto const& [name, value] : metadata.metadata) {
    if (included.contains(name)) {
      selected_size += name.size() + value.size();
      selected.emplace_back(record{{"name", name}, {"value", value}});
    }
  }
  return DecodeContext{.attribute_mode = args.get_attribute_mode(),
                       .peer_ip = peer_ip,
                       .metadata = std::move(selected),
                       .transport = metadata.transport,
                       .receiver_metadata_size = selected_size,
                       .cancellation_requested = {},
                       .warn_duplicate_attribute = {},
                       .duplicate_attribute_warnings_emitted = 0};
}

} // namespace tenzir::plugins::accept_otlp::detail
