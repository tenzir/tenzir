#ifndef VAST_CONCEPT_PARSEABLE_VAST_DETAIL_ESCAPED_STRING_H
#define VAST_CONCEPT_PARSEABLE_VAST_DETAIL_ESCAPED_STRING_H

#include <string>

#include "vast/concept/parseable/vast/detail/boost.h"

namespace vast {
namespace detail {
namespace parser {

namespace qi = boost::spirit::qi;

template <typename Iterator>
struct escaped_string : qi::grammar<Iterator, std::string()>
{
  escaped_string(char id)
    : escaped_string::base_type(str)
    , id(id)
  {
    qi::hex_type hex;
    qi::lit_type lit;
    qi::print_type print;

    auto esc_id = std::string("\\") + id;
    esc.add
      ("\\a", '\a')
      ("\\b", '\b')
      ("\\f", '\f')
      ("\\n", '\n')
      ("\\r", '\r')
      ("\\t", '\t')
      ("\\v", '\v')
      ("\\\\", '\\')
      (esc_id.data(), id)
      ;

    str
      =   lit(id)
      >>  *(esc | "\\x" >> hex | (print - lit(id)))
      >>  lit(id)
      ;
  }

  qi::rule<Iterator, std::string()> str;
  qi::symbols<char const, char const> esc;
  char id;
};

} // namespace parser
} // namespace detail
} // namespace vast

#endif
