#ifndef VAST_VALUE_TYPE_H
#define VAST_VALUE_TYPE_H

#include <iosfwd>
#include <string>
#include "vast/fwd.h"
#include "vast/traits.h"

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
