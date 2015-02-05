#ifndef VAST_KEY_H
#define VAST_KEY_H

#include <string>
#include "vast/print.h"
#include "vast/parse.h"
#include "vast/util/string.h"
#include "vast/util/stack/vector.h"

namespace vast {

/// A sequence of type/argument names to recursively access a type or value.
struct key : util::stack::vector<4, std::string>
{
  using util::stack::vector<4, std::string>::vector;

  template <typename Iterator>
  friend trial<void> parse(key& k, Iterator& begin, Iterator end)
  {
    for (auto& str : util::to_strings(util::split(begin, end, ".")))
      k.push_back(std::move(str));
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
