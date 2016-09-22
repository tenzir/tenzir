#ifndef VAST_CONCEPT_PARSEABLE_STRING_C_STRING_HPP
#define VAST_CONCEPT_PARSEABLE_STRING_C_STRING_HPP

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/detail/assert.hpp"

namespace vast {

class c_string_parser : public parser<c_string_parser> {
public:
  using attribute = char const*;

  c_string_parser(char const* str) : str_{str} {
    VAST_ASSERT(str != nullptr);
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    auto i = f;
    auto p = str_;
    while (*p != '\0')
      if (i == l || *i++ != *p++)
        return false;
    a = str_;
    f = i;
    return true;
  }

private:
  char const* str_;
};

template <>
struct parser_registry<char const*> {
  using type = c_string_parser;
};

} // namespace vast

#endif
