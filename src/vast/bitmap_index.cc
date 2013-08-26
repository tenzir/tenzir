#include "vast/bitmap_index.h"

#include "vast/value.h"

namespace vast {

bool bitmap_index::push_back(value const& val)
{
  return (val == nil) ? append(1, false) : push_back_impl(val);
}

} // namespace vast
