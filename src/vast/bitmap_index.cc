#include "vast/bitmap_index.h"

#include "vast/value.h"

namespace vast {

bool bitmap_index::push_back(value const& val)
{
  return (val == nil) ? patch(1) : push_back_impl(val);
}

} // namespace vast

