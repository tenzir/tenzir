#ifndef VAST_VALUE_TYPE_H
#define VAST_VALUE_TYPE_H

#include <iosfwd>
#include <string>
#include "vast/fwd.h"
#include "vast/traits.h"

namespace vast {

/// The type of a value.
enum value_type : uint8_t
{
  invalid_value    = 0x00, ///< An invalid value.

  // Basic types
  bool_value       = 0x01, ///< A boolean value.
  int_value        = 0x02, ///< An integer (`int64_t`) value.
  uint_value       = 0x03, ///< An unsigned integer (`uint64_t`) value.
  double_value     = 0x04, ///< A floating point (`double`) value.

  // Time types
  time_range_value = 0x05, ///< A time duration value.
  time_point_value = 0x06, ///< A time point value.

  // String types
  string_value     = 0x07, ///< A string value.
  regex_value      = 0x08, ///< A regular expression value.

  // Network types
  address_value    = 0x09, ///< An IP address value.
  prefix_value     = 0x0a, ///< An IP prefix value.
  port_value       = 0x0b, ///< A transport-layer port value.

  // Container types
  record_value     = 0x0c, ///< A sequence of heterogeneous values.
  vector_value     = 0x0d, ///< A sequence of homogeneous values.
  set_value        = 0x0e, ///< A collection of unique values.
  table_value      = 0x0f  ///< A mapping of values to values.
};

/// Checks whether a type is container type.
inline bool is_container_type(value_type t)
{
  return t == record_value
      || t == vector_value
      || t == set_value
      || t == table_value;
}

void serialize(serializer& sink, value_type x);
void deserialize(deserializer& source, value_type& x);

template <typename Iterator>
bool print(Iterator& out, value_type t)
{
  auto str = "unknown";
  switch (t)
  {
    default:
      break;
    case invalid_value:
      str = "invalid";
      break;
    case bool_value:
      str = "bool";
      break;
    case int_value:
      str = "int";
      break;
    case uint_value:
      str = "uint";
      break;
    case double_value:
      str = "double";
      break;
    case time_range_value:
      str = "duration";
      break;
    case time_point_value:
      str = "time";
      break;
    case string_value:
      str = "string";
      break;
    case regex_value:
      str = "regex";
      break;
    case address_value:
      str = "address";
      break;
    case prefix_value:
      str = "prefix";
      break;
    case port_value:
      str = "port";
      break;
    case record_value:
      str = "record";
      break;
    case vector_value:
      str = "vector";
      break;
    case set_value:
      str = "set";
      break;
    case table_value:
      str = "table";
      break;
  }
  // TODO: replace strlen with appropriate compile-time constructs.
  out = std::copy(str, str + std::strlen(str), out);
  return true;
}

std::ostream& operator<<(std::ostream& out, value_type t);

/// Meta function to retrieve the underlying type of a given value type.
/// @tparam T The value type to get the underlying type from.
template <value_type T>
using underlying_value_type =
  IfThenElse<T == invalid_value, value_invalid,
    IfThenElse<T == bool_value, bool,
      IfThenElse<T == int_value, int64_t,
        IfThenElse<T == uint_value, uint64_t,
          IfThenElse<T == double_value, double,
            IfThenElse<T == time_range_value, time_range,
              IfThenElse<T == time_point_value, time_point,
                IfThenElse<T == string_value, string,
                  IfThenElse<T == address_value, address,
                    IfThenElse<T == prefix_value, prefix,
                      IfThenElse<T == port_value, port,
                        IfThenElse<T == record_value, record,
                          IfThenElse<T == vector_value, vector,
                            IfThenElse<T == set_value, set,
                              IfThenElse<T == table_value, table,
                                std::false_type>>>>>>>>>>>>>>>;

} // namespace vast

#endif
