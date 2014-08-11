#ifndef VAST_KEY_H
#define VAST_KEY_H

#include <string>
#include "vast/print.h"
#include "vast/parse.h"
#include "vast/util/stack_vector.h"
#include "vast/util/string.h"

namespace vast {

/// A sequence of type/argument names to recursively access a type or value.
struct key : util::stack_vector<std::string, 4>
{
  using util::stack_vector<std::string, 4>::stack_vector;

  template <typename Iterator>
  friend trial<void> parse(key& k, Iterator& begin, Iterator end)
  {
    k = util::to_strings(util::split(begin, end, "."));
    return nothing;
  }

  template <typename Iterator>
  friend trial<void> print(key const& k, Iterator&& out)
  {
    return print_delimited('.', k.begin(), k.end(), out);
  }
};

} // namespace vast

#endif
