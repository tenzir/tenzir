#ifndef VAST_UTIL_PARSER_PARSE_H
#define VAST_UTIL_PARSER_PARSE_H

#include <vast/util/parser/error_handler.h>
#include <vast/util/parser/skipper.h>

namespace vast {
namespace util {
namespace parser {

template <template <class> class Grammar, typename Attribute>
bool parse(std::string const& str, Attribute& attr)
{
  typedef std::string::const_iterator iterator_type;
  auto i = str.begin();
  auto end = str.end();

  vast::util::parser::error_handler<iterator_type> error_handler(i, end);
  Grammar<iterator_type> grammar(error_handler);
  vast::util::parser::skipper<iterator_type> skipper;

  bool success = phrase_parse(i, end, grammar, skipper, attr);
  return success && i == end;
}

} // namespace parser
} // namespace util
} // namespace vast

#endif
