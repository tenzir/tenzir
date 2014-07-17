#include "vast/detail/demangle.h"
#include "caf/detail/demangle.hpp"

namespace vast {
namespace detail {

std::string demangle(char const* name)
{
  return caf::detail::demangle(name);
}

std::string demangle(const std::type_info& info)
{
  return demangle(info.name());
}

} // namespace detail
} // namespace vast
