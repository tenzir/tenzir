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
  invalid_type    = 0x00, ///< An invalid value.

  // Basic types
  bool_type       = 0x01, ///< A boolean value.
  int_type        = 0x02, ///< An integer (`int`) value.
  uint_type       = 0x03, ///< An unsigned integer (`int`) value.
  double_type     = 0x04, ///< A floating point (`double`) value.
  time_range_type = 0x05, ///< A time duration value.
  time_point_type = 0x06, ///< A time point value.
  string_type     = 0x07, ///< A string value.
  regex_type      = 0x08, ///< A regular expression value.

  // Network types
  address_type    = 0x09, ///< An IP address value.
  prefix_type     = 0x0a, ///< An IP prefix value.
  port_type       = 0x0b, ///< A transport-layer port value.

  // Containers
  record_type     = 0x0c, ///< A sequenced vector value.
  table_type      = 0x0d  ///< An associative array value.
};

/// Checks whether a type is container type.
inline bool is_container_type(value_type t)
{
  return t == record_type || t == table_type;
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
    case invalid_type:
      str = "invalid";
      break;
    case bool_type:
      str = "bool";
      break;
    case int_type:
      str = "int";
      break;
    case uint_type:
      str = "uint";
      break;
    case double_type:
      str = "double";
      break;
    case time_range_type:
      str = "duration";
      break;
    case time_point_type:
      str = "time";
      break;
    case string_type:
      str = "string";
      break;
    case regex_type:
      str = "regex";
      break;
    case address_type:
      str = "address";
      break;
    case prefix_type:
      str = "prefix";
      break;
    case port_type:
      str = "port";
      break;
    case record_type:
      str = "record";
      break;
    case table_type:
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
  IfThenElse<T == invalid_type, invalid_value,
    IfThenElse<T == bool_type, bool,
      IfThenElse<T == int_type, int64_t,
        IfThenElse<T == uint_type, uint64_t,
          IfThenElse<T == double_type, double,
            IfThenElse<T == time_range_type, time_range,
              IfThenElse<T == time_point_type, time_point,
                IfThenElse<T == string_type, string,
                  IfThenElse<T == address_type, address,
                    IfThenElse<T == prefix_type, prefix,
                      IfThenElse<T == port_type, port,
                        IfThenElse<T == record_type, record,
                          IfThenElse<T == table_type, table,
                            std::false_type>>>>>>>>>>>>>;

} // namespace vast

#endif
