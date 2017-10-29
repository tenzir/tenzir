#ifndef VAST_CONCEPT_PARSEABLE_CORE_REPEAT_HPP
#define VAST_CONCEPT_PARSEABLE_CORE_REPEAT_HPP

#include <vector>

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/detail/container.hpp"

#include "vast/detail/assert.hpp"

namespace vast {
namespace detail {

template <class Parser, class Iterator, class Attribute>
bool parse_repeat(Parser& p, Iterator& f, Iterator const& l, Attribute& a,
                  int min, int max) {
  if (max == 0)
    return true; // If we have nothing todo, we're succeeding.
  auto save = f;
  auto i = 0;
  while (i < max) {
    if (!container<typename Parser::attribute>::parse(p, f, l, a))
      break;
    ++i;
  }
  if (i >= min)
    return true;
  f = save;
  return false;
}

} // namespace detail

template <class Parser, int Min, int Max = Min>
class static_repeat_parser
  : public parser<static_repeat_parser<Parser, Min, Max>> {
  static_assert(Min <= Max, "minimum must be smaller than maximum");

public:
  using container = detail::container<typename Parser::attribute>;
  using attribute = typename container::attribute;

  explicit static_repeat_parser(Parser p) : parser_{std::move(p)} {
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    return detail::parse_repeat(parser_, f, l, a, Min, Max);
  }

private:
  Parser parser_;
};

template <class Parser>
class dynamic_repeat_parser : public parser<dynamic_repeat_parser<Parser>> {
public:
  using container = detail::container<typename Parser::attribute>;
  using attribute = typename container::attribute;

  dynamic_repeat_parser(Parser p, const int& min, const int& max)
    : parser_{std::move(p)},
      min_{min},
      max_{max} {
    VAST_ASSERT(min <= max);
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    return detail::parse_repeat(parser_, f, l, a, min_, max_);
  }

private:
  Parser parser_;
  const int& min_;
  const int& max_;
};

template <int Min, int Max = Min, class Parser>
auto repeat(Parser const& p) {
  return static_repeat_parser<Parser, Min, Max>{p};
}

template <class Parser>
auto repeat(Parser const& p, int n) {
  return dynamic_repeat_parser<Parser>{p, n, n};
}

template <class Parser>
auto repeat(Parser const& p, int min, int max) {
  return dynamic_repeat_parser<Parser>{p, min, max};
}

namespace parsers {

template <int Min, int Max = Min, class Parser>
auto rep(Parser const& p) {
  return repeat<Min, Max>(p);
}

template <class Parser>
auto rep(Parser const& p, const int& n) {
  return repeat(p, n);
}

template <class Parser>
auto rep(Parser const& p, const int& min, const int& max) {
  return repeat(p, min, max);
}

} // namespace parsers
} // namespace vast

#endif
