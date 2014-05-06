#ifndef VAST_KEY_H
#define VAST_KEY_H

#include <vector>
#include "vast/print.h"
#include "vast/parse.h"
#include "vast/string.h"

namespace vast {

/// A sequence of type/argument names to recursively access a type or value.
struct key : std::vector<string>
{
  using super = std::vector<string>;

  key() = default;

  key(super::const_iterator begin, super::const_iterator end)
    : super{begin, end}
  {
  }

  key(super v)
    : super{std::move(v)}
  {
  }

  key(std::initializer_list<string> list)
    : super{std::move(list)}
  {
  }

  template <typename Iterator>
  friend trial<void> parse(key& k, Iterator& begin, Iterator end)
  {
    auto str = parse<string>(begin, end);
    if (! str)
      return str.error();

    for (auto& p : str->split("."))
      k.emplace_back(p.first, p.second);

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
