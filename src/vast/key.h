#ifndef VAST_KEY_H
#define VAST_KEY_H

#include <string>
#include "vast/print.h"
#include "vast/util/string.h"
#include "vast/util/stack/vector.h"

namespace vast {

/// A sequence of type/argument names to recursively access a type or value.
struct key : util::stack::vector<4, std::string>
{
  using super = util::stack::vector<4, std::string>;
  using super::vector;
};

// TODO: Migrate to concepts location.
template <typename Iterator>
trial<void> print(key const& k, Iterator&& out)
{
  return print_delimited('.', k.begin(), k.end(), out);
}

} // namespace vast

#endif
