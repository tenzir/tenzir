//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "broker/zeek.hpp"

#include <vast/address.hpp>
#include <vast/as_bytes.hpp>
#include <vast/concept/printable/to_string.hpp>
#include <vast/concept/printable/vast/data.hpp>
#include <vast/detail/byte_swap.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/detail/type_traits.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/type.hpp>

namespace vast::plugins::broker {

// Things we take directly from zeek.
namespace zeek {

enum class tag : int {
  type_void,     // 0
  type_bool,     // 1
  type_int,      // 2
  type_count,    // 3
  type_counter,  // 4
  type_double,   // 5
  type_time,     // 6
  type_interval, // 7
  type_string,   // 8
  type_pattern,  // 9
  type_enum,     // 10
  type_timer,    // 11
  type_port,     // 12
  type_addr,     // 13
  type_subnet,   // 14
  type_any,      // 15
  type_table,    // 16
  type_union,    // 17
  type_record,   // 18
  type_list,     // 19
  type_func,     // 20
  type_file,     // 21
  type_vector,   // 22
  type_opaque,   // 23
  type_type,     // 24
  type_error,    // 25
  MAX = type_error,
};

/// Parses a value out of binary Zeek data.
/// @param bytes The raw bytes to parse.
/// @returns An error on failure.
/// @post *bytes* is advanced by the number of bytes of the extract value.
template <class T>
caf::error extract(T& x, span<const std::byte>& bytes) {
  if constexpr (std::is_same_v<T, char>) {
    if (bytes.empty())
      return caf::make_error(ec::parse_error, "input exhausted");
    x = static_cast<char>(bytes[0]);
    bytes = bytes.subspan(1);
  } else if constexpr (std::is_same_v<T, bool>) {
    char c;
    if (auto err = extract(c, bytes))
      return err;
    x = (c == '\1');
  } else if constexpr (std::is_same_v<T, int>) {
    // In Zeek, an int has always 32 bits on the wire.
    uint32_t result;
    if (auto err = extract(result, bytes))
      return err;
    x = static_cast<int>(result);
  } else if constexpr (std::is_same_v<T, double>) {
    if (bytes.size() < sizeof(T))
      return caf::make_error(ec::parse_error, "input exhausted");
    std::memcpy(&x, bytes.data(), sizeof(T));
    // Directly lifted from src/net_util.h.
    static auto ntohd = [](double d) {
      VAST_ASSERT(sizeof(d) == 8);
      double tmp;
      char* src = (char*)&d;
      char* dst = (char*)&tmp;
      dst[0] = src[7];
      dst[1] = src[6];
      dst[2] = src[5];
      dst[3] = src[4];
      dst[4] = src[3];
      dst[5] = src[2];
      dst[6] = src[1];
      dst[7] = src[0];
      return tmp;
    };
    x = ntohd(x);
    bytes = bytes.subspan(sizeof(T));
  } else if constexpr (std::is_integral_v<T>) {
    std::make_unsigned_t<T> u;
    if (bytes.size() < sizeof(T))
      return caf::make_error(ec::parse_error, "input exhausted");
    std::memcpy(&u, bytes.data(), sizeof(T));
    x = detail::narrow_cast<T>(detail::to_host_order(u));
    bytes = bytes.subspan(sizeof(T));
  } else if constexpr (std::is_same_v<T, std::string>) {
    uint32_t length = 0;
    if (auto err = extract(length, bytes))
      return err;
    if (length > bytes.size())
      return caf::make_error(ec::parse_error, "input exhausted");
    x.resize(length);
    std::memcpy(x.data(), bytes.data(), x.size());
    bytes = bytes.subspan(x.size());
  } else if constexpr (std::is_same_v<T, address>) {
    char family;
    if (auto err = extract(family, bytes))
      return err;
    switch (family) {
      default:
        return caf::make_error(ec::parse_error, "invalid addr family", family);
      case 4: {
        if (bytes.size() < 4)
          return caf::make_error(ec::parse_error, "input exhausted");
        x = address::v4(bytes.data(), address::byte_order::network);
        bytes = bytes.subspan(4);
        break;
      }
      case 6:
        if (bytes.size() < 16)
          return caf::make_error(ec::parse_error, "input exhausted");
        x = address::v6(bytes.data(), address::byte_order::network);
        bytes = bytes.subspan(16);
        break;
    }
  }
  return {};
}

/// Parses a binary Zeek value.
/// TODO: add an output parameter or function similar to extract above.
caf::error extract_value(span<const std::byte>& bytes) {
  // Every value begins with type information.
  int type, sub_type;
  bool present;
  auto err = caf::error::eval(
    [&] {
      return extract(type, bytes);
    },
    [&] {
      return extract(sub_type, bytes);
    },
    [&] {
      return extract(present, bytes);
    });
  if (err)
    return err;
  // Skip null values.
  if (!present) {
    VAST_INFO("nil");
    return {};
  }
  // Dispatch on the Zeek tag type.
  switch (static_cast<zeek::tag>(type)) {
    default:
      return caf::make_error(ec::parse_error, "unsupported value type", type);
    case zeek::tag::type_bool: {
      int64_t x;
      if (auto err = extract(x, bytes))
        return err;
      VAST_INFO("bool = {}", !!x);
      break;
    }
    case zeek::tag::type_int: {
      int64_t x;
      if (auto err = extract(x, bytes))
        return err;
      VAST_INFO("int = {}", x);
      break;
    }
    case zeek::tag::type_count:
    case zeek::tag::type_counter: {
      uint64_t x;
      if (auto err = extract(x, bytes))
        return err;
      VAST_INFO("count = {}", x);
      break;
    }
    case zeek::tag::type_port: {
      uint64_t number;
      int proto;
      if (auto err = extract(number, bytes))
        return err;
      if (auto err = extract(proto, bytes))
        return err;
      VAST_INFO("port = {}/{}", number, proto);
      break;
    }
    case zeek::tag::type_addr: {
      address addr;
      if (auto err = extract(addr, bytes))
        return err;
      VAST_INFO("addr = {}", addr);
      break;
    }
    case zeek::tag::type_subnet: {
      uint8_t length;
      if (auto err = extract(length, bytes))
        return err;
      address addr;
      if (auto err = extract(addr, bytes))
        return err;
      VAST_INFO("subnet = {}/{}", addr, length);
      break;
    }
    case zeek::tag::type_double:
    case zeek::tag::type_time:
    case zeek::tag::type_interval: {
      double x;
      if (auto err = extract(x, bytes))
        return err;
      VAST_INFO("double = {}", x);
      break;
    }
    case zeek::tag::type_enum:
    case zeek::tag::type_string:
    case zeek::tag::type_file:
    case zeek::tag::type_func: {
      std::string x;
      if (auto err = extract(x, bytes))
        return err;
      VAST_INFO("string = {}", x);
      break;
    }
    case zeek::tag::type_table: {
      int64_t size;
      if (auto err = extract(size, bytes))
        return err;
      VAST_INFO("table of size {}", size);
      for (auto i = 0; i < size; ++i)
        if (auto err = extract_value(bytes))
          return err;
      break;
    }
    case zeek::tag::type_vector: {
      int64_t size;
      if (auto err = extract(size, bytes))
        return err;
      VAST_INFO("vector of size {}", size);
      for (auto i = 0; i < size; ++i)
        if (auto err = extract_value(bytes))
          return err;
      break;
    }
  }
  return {};
}

/// The equivalent of threading::Field from the perspective of Broker.
/// @note We don't need all fields, e.g., only the input framework uses the
/// "secondary name", and "optional" is everyting in VAST.
struct field {
  std::string_view name;
  tag type;
  tag sub_type;
};

/// Parses a Zeek field from a Broker data instance.
caf::error extract(field& x, const ::broker::data& data) {
  const auto xs = caf::get_if<::broker::vector>(&data);
  if (!xs)
    return caf::make_error(ec::parse_error, "field not a vector");
  if (xs->size() != 5)
    return caf::make_error(ec::parse_error, "invalid field info");
  const auto& fields = *xs;
  const auto& name = caf::get_if<std::string>(&fields[0]);
  const auto& type = caf::get_if<uint64_t>(&fields[2]);
  const auto& sub_type = caf::get_if<uint64_t>(&fields[3]);
  if (!name)
    return caf::make_error(ec::parse_error, "name not a string");
  if (!type)
    return caf::make_error(ec::parse_error, "type not a uint64_t");
  if (!sub_type)
    return caf::make_error(ec::parse_error, "sub_type not a uint64_t");
  x.name = *name;
  x.type = detail::narrow_cast<tag>(*type);
  x.sub_type = detail::narrow_cast<tag>(*sub_type);
  return {};
}

/// Creates a VAST type from two Zeek type tags. Indeed, this is a partial
/// function but the subset of Zeek's threading values that can show up in logs
/// is quite limited, so it does cover all cases we encounter in practice.
caf::error convert(tag type, tag sub_type, vast::type& result) {
  switch (type) {
    default:
      return caf::make_error(ec::parse_error, "unsupported value type",
                             static_cast<int>(type));
    case tag::type_bool:
      result = bool_type{};
      break;
    case tag::type_int:
      result = integer_type{};
      break;
    case tag::type_count:
    case tag::type_counter:
      result = count_type{};
      break;
    case tag::type_port:
      // TODO: is there a pre-defined type alias called port in libvast?
      result = count_type{}.name("port");
      break;
    case tag::type_addr:
      result = address_type{};
      break;
    case tag::type_subnet:
      result = subnet_type{};
      break;
    case tag::type_double:
      result = real_type{};
      break;
    case tag::type_time:
      result = time_type{};
      break;
    case tag::type_interval:
      result = duration_type{};
      break;
    case tag::type_enum:
      // FIXME: unless we know all possible values a priori, we cannot use
      // enmuration_type here. Not sure how to go after this. Right now we
      // treat enums as strings. --MV
      result = string_type{}.attributes({{"index", "hash"}});
      break;
    case tag::type_string:
      result = string_type{};
      break;
    case tag::type_table:
    case tag::type_vector: {
      // Zeek's threading values do not support tables/maps. We can treat them
      // as vectors. To avoid losing the set semantics, we can either have a
      // type alias or add a type attribute.
      vast::type element_type;
      if (auto err = convert(sub_type, tag::type_error, element_type))
        return err;
      // Retain set semantics for tables.
      if (sub_type == tag::type_table)
        element_type.name("set");
      result = list_type{std::move(element_type)};
      break;
    }
  }
  return {};
}

} // namespace zeek

caf::expected<type> process(const ::broker::zeek::LogCreate& msg) {
  // Parse Zeek's WriterBackend::WriterInfo.
  if (!msg.valid())
    return caf::make_error(ec::parse_error, "invalid log create message");
  const auto writer_info = caf::get_if<::broker::vector>(&msg.writer_info());
  if (!writer_info)
    return caf::make_error(ec::parse_error, "writer_info not a vector");
  if (writer_info->size() != 6)
    return caf::make_error(ec::parse_error, "invalid writer_info");
  const auto type_name = caf::get_if<std::string>(&writer_info->front());
  if (!type_name)
    return caf::make_error(ec::parse_error, "type name not a string");
  const auto rotation_base_dbl = caf::get_if<double>(&(*writer_info)[1]);
  if (!rotation_base_dbl)
    return caf::make_error(ec::parse_error, "rotation_base not a double");
  const auto rotation_interval_dbl = caf::get_if<double>(&(*writer_info)[2]);
  if (!rotation_interval_dbl)
    return caf::make_error(ec::parse_error, "rotation_interval not a double");
  const auto network_time_dbl = caf::get_if<double>(&(*writer_info)[3]);
  if (!network_time_dbl)
    return caf::make_error(ec::parse_error, "network_time not a double");
  const auto& fields_data = caf::get_if<::broker::vector>(&msg.fields_data());
  if (!fields_data)
    return caf::make_error(ec::parse_error, "fields_data not a vector");
  const auto config = caf::get_if<::broker::table>(&(*writer_info)[4]);
  if (!config)
    return caf::make_error(ec::parse_error, "config not a table");
  // Log filters are Zeek functions, which VAST cannot handle.
  for (const auto& [key, value] : *config)
    VAST_WARN("ignoring Zeek log filter: {} = {}", key, value);
  // Parse timestamps.
  static auto double_to_duration = [](double x) {
    return std::chrono::duration_cast<duration>(double_seconds{x});
  };
  auto rotation_base = time{double_to_duration(*rotation_base_dbl)};
  auto rotation_interval = double_to_duration(*rotation_interval_dbl);
  auto network_time = time{double_to_duration(*network_time_dbl)};
  VAST_DEBUG("creating Zeek log: stream={}, type={} "
             "rotation_base={} rotation_interval={} created={}",
             msg.stream_id(), *type_name, rotation_base, rotation_interval,
             network_time);
  // Create a VAST type from here.
  std::vector<record_field> fields;
  fields.reserve(fields_data->size());
  for (const auto& x : *fields_data) {
    zeek::field field;
    if (auto err = extract(field, x))
      return err;
    type field_type;
    if (auto err = convert(field.type, field.sub_type, field_type))
      return err;
    fields.emplace_back(std::string{field.name}, std::move(field_type));
  }
  return record_type{std::move(fields)}.name("zeek." + *type_name);
}

caf::error process(const ::broker::zeek::LogWrite& msg) {
  auto serial_data = caf::get_if<std::string>(&msg.serial_data());
  if (!serial_data) {
    VAST_WARN("got invalid LogWrite serial data");
    return ec::parse_error;
  }
  auto bytes = as_bytes(*serial_data);
  // Read the number of fields.
  uint32_t num_fields;
  if (auto err = zeek::extract(num_fields, bytes))
    return err;
  // Read as many "threading values" as there are fields.
  for (size_t i = 0; i < num_fields; ++i) {
    if (auto err = zeek::extract_value(bytes))
      return err;
  }
  if (bytes.size() > 0)
    VAST_ERROR("incomplete read, {} bytes remaining", bytes.size());
  return {};
}

} // namespace vast::plugins::broker
