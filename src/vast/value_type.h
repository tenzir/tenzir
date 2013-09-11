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
  invalid_type    = 0x00, ///< The value has not been initialized.
  nil_type        = 0x01, ///< The sentinel (empty) value type.

  // Basic types
  bool_type       = 0x02, ///< A boolean value.
  int_type        = 0x03, ///< An integer (@c int) value.
  uint_type       = 0x04, ///< An unsigned integer (@c int) value.
  double_type     = 0x05, ///< A floating point (@c double) value.
  time_range_type = 0x06, ///< A time duration value.
  time_point_type = 0x07, ///< A time point value.
  string_type     = 0x08, ///< A string value.
  regex_type      = 0x09, ///< A regular expression value.

  // Containers
  vector_type     = 0x0a, ///< A vector value.
  set_type        = 0x0b, ///< A set value.
  table_type      = 0x0c, ///< A table value.
  record_type     = 0x0d, ///< A record value.

  // Network types
  address_type    = 0x0e, ///< An IP address value.
  prefix_type     = 0x0f, ///< An IP prefix value.
  port_type       = 0x10  ///< A transport-layer port value.
};

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
    case nil_type:
      str = "nil";
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
    case vector_type:
      str = "vector";
      break;
    case set_type:
      str = "set";
      break;
    case table_type:
      str = "table";
      break;
    case record_type:
      str = "record";
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
  }
  // TODO: replace strlen with appropriate compile-time constructs.
  out = std::copy(str, str + std::strlen(str), out);
  return true;
}

/// Meta function to retrieve the underlying type of a given value type.
/// @tparam T The value type to get the underlying type from.
template <value_type T>
using underlying_value_type =
  IfThenElse<T == invalid_type, invalid_value,
    IfThenElse<T == nil_type, nil_value,
      IfThenElse<T == bool_type, bool,
        IfThenElse<T == int_type, int64_t,
          IfThenElse<T == uint_type, uint64_t,
            IfThenElse<T == double_type, double,
              IfThenElse<T == time_range_type, time_range,
                IfThenElse<T == time_point_type, time_point,
                  IfThenElse<T == string_type, string,
                    IfThenElse<T == vector_type, vector,
                      IfThenElse<T == set_type, set,
                        IfThenElse<T == table_type, table,
                          IfThenElse<T == record_type, record,
                            IfThenElse<T == address_type, address,
                              IfThenElse<T == prefix_type, prefix,
                                IfThenElse<T == port_type, port,
                                  std::false_type>>>>>>>>>>>>>>>>;

} // namespace vast

#endif
