#ifndef VAST_CONCEPT_PARSEABLE_CORE_REPEAT_H
#define VAST_CONCEPT_PARSEABLE_CORE_REPEAT_H

#include <vector>

#include "vast/concept/parseable/core/parser.h"
#include "vast/concept/parseable/detail/container.h"

namespace vast {

template <typename Parser, int Min, int Max = Min>
class repeat_parser : parser<repeat_parser<Parser, Min, Max>>
{
  static_assert(Min <= Max, "minimum must be smaller than maximum");

public:
  using container = detail::container<typename Parser::attribute>;
  using attribute = typename container::attribute;

  explicit repeat_parser(Parser p)
    : parser_{std::move(p)}
  {
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const
  {
    if (Max == 0)
      return true; // If we have nothing todo, we're succeeding.
    auto save = f;
    auto i = 0;
    while (i < Max)
    {
      if (! container::parse(parser_, f, l, a))
        break;
      ++i;
    }
    if (i >= Min)
      return true;
    f = save;
    return false;
  }

private:
  Parser parser_;
};

template <int Min, int Max = Min, typename Parser>
auto repeat(Parser const& p)
{
  return repeat_parser<Parser, Min, Max>{p};
}

namespace parsers {

template <int Min, int Max = Min, typename Parser>
auto rep(Parser const& p)
{
  return repeat<Min, Max, Parser>(p);
}

} // namespace parsers
} // namespace vast

#endif
