#ifndef VAST_CONCEPT_PARSEABLE_STRING_STRING_H
#define VAST_CONCEPT_PARSEABLE_STRING_STRING_H

#include <cassert>
#include <string>
#include "vast/concept/parseable/core/parser.h"

namespace vast {

class string_parser : public parser<string_parser>
{
public:
  using attribute = std::string;

  string_parser(std::string const& str)
    : str_{str}
  {
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const
  {
    auto i = f;
    auto begin = str_.begin();
    auto end = str_.end();
    while (begin != end)
      if (i == l || *i++ != *begin++)
        return false;
    f = i;
    return true;
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const
  {
    a.clear();
    auto out = std::back_inserter(a);
    auto i = f;
    auto begin = str_.begin();
    auto end = str_.end();
    while (begin != end)
      if (i == l || *i != *begin++)
        return false;
      else
        *out++ = *i++;
    f = i;
    return true;
  }

private:
  std::string str_;
};

template <>
struct parser_registry<std::string>
{
  using type = string_parser;
};

} // namespace vast

#endif


