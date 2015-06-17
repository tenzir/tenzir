#ifndef VAST_CONCEPT_PARSEABLE_CORE_REPEAT_H
#define VAST_CONCEPT_PARSEABLE_CORE_REPEAT_H

#include <vector>

#include "vast/concept/parseable/core/parser.h"

namespace vast {

template <typename Parser, int Min, int Max = Min>
class repeat_parser : parser<repeat_parser<Parser, Min, Max>>
{
  static_assert(Min <= Max, "minimum must be smaller than maximum");

public:
  using attribute = std::vector<typename Parser::attribute>;

  repeat_parser(Parser const& p)
    : parser_{p}
  {
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const
  {
    if (Max == 0)
      return true; // If we have nothing todo, we're succeeding.
    auto init = f;
    auto i = 0;
    while (i < Max)
    {
      auto save = f;
      if (! parser_.parse(f, l, a))
      {
        f = save;
        break;
      }
      ++i;
    }
    if (i >= Min)
      return true;
    f = init;
    return false;
  }

private:
  Parser parser_;
};

template <int Min, int Max = Min, typename Parser>
repeat_parser<Parser, Min, Max> repeat(Parser const& p)
{
  return p;
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
