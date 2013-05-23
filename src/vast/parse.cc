#include "vast/parse.h"

#include "vast/detail/parser/value.h"

namespace vast {

bool parse(std::string const& str, value& val)
{
  typedef std::string::const_iterator iterator_type;
  detail::parser::value<iterator_type> parser;
  detail::parser::skipper<iterator_type> skipper;

  auto i = str.begin();
  auto end = str.end();
  bool success = phrase_parse(i, end, parser, skipper, val);

  if (success && i == end)
    return true;

  val = invalid;
  return false;
}

} // namespace vast
