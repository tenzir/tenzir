#include "vast/detail/parse.h"

#include <cstdlib>

namespace vast {
namespace detail {

double to_double(char const* str)
{
  return std::atof(str);
}

} // namespace detail
} // namespace vast
