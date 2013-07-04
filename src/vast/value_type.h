#ifndef VAST_VALUE_TYPE_H
#define VAST_VALUE_TYPE_H

#include <iosfwd>
#include <string>
#include <type_traits>
#include "vast/fwd.h"

namespace vast {

// Forward declarations.
struct invalid_value;
struct nil_value;
class time_range;
class time_point;
class string;
class vector;
class set;
class table;
class record;
class address;
class port;
class prefix;

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

std::string to_string(value_type t);
std::ostream& operator<<(std::ostream& out, value_type const& t);

/// Synactic sugar for `std::conditional`.
template <bool If, typename Then, typename Else>
using Condition = typename std::conditional<If, Then, Else>::type;

/// Meta function to retrieve the underlying type of a given value type.
/// @tparam T The value type to get the underlying type from.
template <value_type T>
using underlying_value_type =
  Condition<T == invalid_type, invalid_value,
    Condition<T == nil_type, nil_value,
      Condition<T == bool_type, bool,
        Condition<T == int_type, int64_t,
          Condition<T == uint_type, uint64_t,
            Condition<T == double_type, double,
              Condition<T == time_range_type, time_range,
                Condition<T == time_point_type, time_point,
                  Condition<T == string_type, string,
                    Condition<T == vector_type, vector,
                      Condition<T == set_type, set,
                        Condition<T == table_type, table,
                          Condition<T == record_type, record,
                            Condition<T == address_type, address,
                              Condition<T == prefix_type, prefix,
                                Condition<T == port_type, port,
                                  std::false_type>>>>>>>>>>>>>>>>;
                  
} // namespace vast

#endif
