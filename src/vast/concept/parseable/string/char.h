#ifndef VAST_CONCEPT_PARSEABLE_STRING_CHAR_H
#define VAST_CONCEPT_PARSEABLE_STRING_CHAR_H

#include <string>

#include "vast/concept/parseable/core/parser.h"

namespace vast {

class char_parser : public parser<char_parser>
{
public:
  using attribute = char;

  char_parser(char c)
    : c_{c}
  {
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const
  {
    if (f == l || *f != c_)
      return false;
    incorporate(a, c_);
    ++f;
    return true;
  }

private:
  template <typename Attribute>
  static void incorporate(Attribute& a, char c)
  {
    a = c;
  }

  static void incorporate(std::string& str, char c)
  {
    str += c;
  }

  static void incorporate(std::vector<char> v, char c)
  {
    v.push_back(c);
  }

  char c_;
};

template <>
struct parser_registry<char>
{
  using type = char_parser;
};

} // namespace vast

#endif

