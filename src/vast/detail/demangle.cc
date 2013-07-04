#include "vast/detail/demangle.h"
#include "cppa/detail/demangle.hpp"

namespace vast {
namespace detail {

std::string demangle(char const* name)
{
  return cppa::detail::demangle(name);
}

std::string demangle(const std::type_info& info)
{
  return demangle(info.name());
}

} // namespace detail
} // namespace vast
