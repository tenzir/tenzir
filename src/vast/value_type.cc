#include "vast/value_type.h"

#include "vast/logger.h"
#include "vast/io/serialization.h"

namespace vast {

void serialize(io::serializer& sink, value_type x)
{
  VAST_ENTER("value_type: " << VAST_ARG(x));
  sink << static_cast<std::underlying_type<value_type>::type>(x);
}

void deserialize(io::deserializer& source, value_type& x)
{
  VAST_ENTER();
  std::underlying_type<value_type>::type u;
  source >> u;
  x = static_cast<value_type>(u);
  VAST_LEAVE("value_type: " << x);
}

std::string to_string(value_type t)
{
  std::string str;
  switch (t)
  {
    default:
      str += "unknown";
      break;
    case invalid_type:
      str += "invalid";
      break;
    case nil_type:
      str += "nil";
      break;
    case bool_type:
      str += "bool";
      break;
    case int_type:
      str += "int";
      break;
    case uint_type:
      str += "uint";
      break;
    case double_type:
      str += "double";
      break;
    case time_range_type:
      str += "duration";
      break;
    case time_point_type:
      str += "time";
      break;
    case string_type:
      str += "string";
      break;
    case regex_type:
      str += "regex";
      break;
    case vector_type:
      str += "vector";
      break;
    case set_type:
      str += "set";
      break;
    case table_type:
      str += "table";
      break;
    case record_type:
      str += "record";
      break;
    case address_type:
      str += "address";
      break;
    case prefix_type:
      str += "prefix";
      break;
    case port_type:
      str += "port";
      break;
  }
  return str;
}

std::ostream& operator<<(std::ostream& out, value_type const& t)
{
  out << to_string(t);
  return out;
}

} // namespace vast
