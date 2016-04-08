#ifndef VAST_CONCEPT_PARSEABLE_FROM_STRING_HPP
#define VAST_CONCEPT_PARSEABLE_FROM_STRING_HPP

#include <type_traits>

#include "vast/trial.hpp"
#include "vast/concept/parseable/core/parse.hpp"

namespace vast {

template <
  typename To,
  typename Parser = make_parser<To>,
  typename Iterator,
  typename... Args
>
auto from_string(Iterator begin, Iterator end, Args&&... args)
  -> std::enable_if_t<is_parseable<Iterator, To>::value, trial<To>> {
  trial<To> t{To{}};
  if (Parser{std::forward<Args>(args)...}.parse(begin, end, *t))
    return t;
  return error{"parsing failed"};
}

template <
  typename To,
  typename Parser = make_parser<To>,
  typename... Args
>
auto from_string(std::string const& str, Args&&... args) {
  auto f = str.begin();
  auto l = str.end();
  return from_string<To, Parser, std::string::const_iterator, Args...>(
    f, l, std::forward<Args>(args)...);
}

template <
  typename To,
  typename Parser = make_parser<To>,
  size_t N,
  typename... Args
>
auto from_string(char const (&str)[N], Args&&... args) {
  auto f = str;
  auto l = str + N - 1; // No NUL byte.
  return from_string<To, Parser, char const*, Args...>(
    f, l, std::forward<Args>(args)...);
}

} // namespace vast

#endif
