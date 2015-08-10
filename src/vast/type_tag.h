#ifndef VAST_TYPE_TAG_H
#define VAST_TYPE_TAG_H

#include <iosfwd>
#include <string>

#include "vast/fwd.h"
#include "vast/print.h"

namespace vast {

/// The type of a value.
enum type_tag : uint8_t {
  invalid_value    = 0x00, ///< An invalid value.

  // Basic types
  bool_value       = 0x01, ///< A boolean value.
  int_value        = 0x02, ///< An integer (`int64_t`) value.
  uint_value       = 0x03, ///< An unsigned integer (`uint64_t`) value.
  double_value     = 0x04, ///< A floating point (`double`) value.
  time_range_value = 0x05, ///< A time duration value.
  time_point_value = 0x06, ///< A time point value.
  string_value     = 0x07, ///< A string value.
  regex_value      = 0x08, ///< A regular expression value.
  address_value    = 0x09, ///< An IP address value.
  prefix_value     = 0x0a, ///< An IP prefix value.
  port_value       = 0x0b, ///< A transport-layer port value.

  // Container types
  vector_value     = 0x0c, ///< A sequence of homogeneous values.
  set_value        = 0x0d, ///< A collection of unique values.
  table_value      = 0x0e, ///< A mapping of values to values.
  record_value     = 0x0f  ///< A sequence of heterogeneous values.
};

/// Checks whether a type tag is a container.
constexpr bool is_container(type_tag t) {
  return t == vector_value || t == set_value || t == table_value;
}

/// Checks whether a type tag is basic.
constexpr bool is_basic(type_tag t) {
  return t != invalid_value && ! is_container(t) && t != record_value;
}

/// Checks whether a type tag is arithmetic.
constexpr bool is_arithmetic(type_tag t) {
  return t > 0x00 && t < 0x07;
}

void serialize(serializer& sink, type_tag x);
void deserialize(deserializer& source, type_tag& x);

template <typename Iterator>
trial<void> print(type_tag t, Iterator&& out) {
  switch (t)
  {
    default:
      return print("unknown", out);
    case invalid_value:
      return print("invalid", out);
    case bool_value:
      return print("bool", out);
    case int_value:
      return print("int", out);
    case uint_value:
      return print("uint", out);
    case double_value:
      return print("double", out);
    case time_range_value:
      return print("duration", out);
    case time_point_value:
      return print("time", out);
    case string_value:
      return print("string", out);
    case regex_value:
      return print("regex", out);
    case address_value:
      return print("address", out);
    case prefix_value:
      return print("prefix", out);
    case port_value:
      return print("port", out);
    case record_value:
      return print("record", out);
    case vector_value:
      return print("vector", out);
    case set_value:
      return print("set", out);
    case table_value:
      return print("table", out);
  }
}

/// Meta function to retrieve the underlying type of a given type tag.
/// @tparam T The type tag to get the underlying type from.
template <type_tag T>
using type_tag_type =
    std::conditional_t<T == invalid_value, value_invalid,
    std::conditional_t<T == bool_value, bool,
    std::conditional_t<T == int_value, int64_t,
    std::conditional_t<T == uint_value, uint64_t,
    std::conditional_t<T == double_value, double,
    std::conditional_t<T == time_range_value, time_range,
    std::conditional_t<T == time_point_value, time_point,
    std::conditional_t<T == string_value, string,
    std::conditional_t<T == address_value, address,
    std::conditional_t<T == prefix_value, prefix,
    std::conditional_t<T == port_value, port,
    std::conditional_t<T == record_value, record,
    std::conditional_t<T == vector_value, vector,
    std::conditional_t<T == set_value, set,
    std::conditional_t<T == table_value, table,
    std::false_type>>>>>>>>>>>>>>>;

} // namespace vast

#endif
