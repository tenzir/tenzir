#ifndef VAST_DETAIL_PARSER_PARSE_H
#define VAST_DETAIL_PARSER_PARSE_H

#include "vast/detail/parser/error_handler.h"
#include "vast/detail/parser/skipper.h"

namespace vast {
namespace detail {
namespace parser {

template <template <class> class Grammar, typename Attribute>
bool parse(std::string const& str, Attribute& attr)
{
  typedef std::string::const_iterator iterator_type;
  auto i = str.begin();
  auto end = str.end();

  error_handler<iterator_type> on_error(i, end);
  Grammar<iterator_type> grammar(on_error);
  skipper<iterator_type> skipper;

  bool success = phrase_parse(i, end, grammar, skipper, attr);
  return success && i == end;
}

} // namespace parser
} // namespace detail
} // namespace vast

#endif
