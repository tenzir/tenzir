#ifndef VAST_DETAIL_PARSER_VALUE_H
#define VAST_DETAIL_PARSER_VALUE_H

#include "vast/detail/parser/escaped_string.h"
#include "vast/detail/parser/address.h"
#include "vast/detail/parser/duration.h"
#include "vast/detail/parser/port.h"
#include "vast/detail/parser/skipper.h"
#include "vast/detail/parser/time_point.h"
#include "vast/value.h"

namespace vast {
namespace detail {
namespace parser {

template <typename Iterator>
struct value : qi::grammar<Iterator, vast::value(), skipper<Iterator>>
{
  value();

  qi::rule<Iterator, vast::value(), skipper<Iterator>> val;
  qi::rule<Iterator, record(), skipper<Iterator>> rec;
  qi::rule<Iterator, vector(), skipper<Iterator>> vec;
  qi::rule<Iterator, set(), skipper<Iterator>> st;
  qi::rule<Iterator, table(), skipper<Iterator>> tbl;

  duration<Iterator> time_dur;
  time_point<Iterator> time_pt;
  address<Iterator> addr;
  port<Iterator> prt;
  escaped_string<Iterator> str, rx;
};

} // namespace parser
} // namespace detail
} // namespace vast

#endif
