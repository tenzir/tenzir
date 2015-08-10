#ifndef VAST_KEY_H
#define VAST_KEY_H

#include <string>

#include "vast/util/stack/vector.h"

namespace vast {

/// A sequence of type/argument names to recursively access a type or value.
struct key : util::stack::vector<4, std::string> {
  using super = util::stack::vector<4, std::string>;
  using super::vector;
};

} // namespace vast

#endif
