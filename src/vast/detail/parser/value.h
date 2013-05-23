#ifndef VAST_DETAIL_PARSER_VALUE_H
#define VAST_DETAIL_PARSER_VALUE_H

// Improves compile times significantly at the cost of predefining terminals.
#define BOOST_SPIRIT_NO_PREDEFINED_TERMINALS

// Turn on to debug the parse process. All productions that should be debugged
// must occur in the BOOST_SPIRIT_DEBUG_NODES macro specified in the grammar
// constructor..
#undef BOOST_SPIRIT_QI_DEBUG

#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix.hpp>
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
  qi::rule<Iterator, vector(), skipper<Iterator>> vector;
  qi::rule<Iterator, set(), skipper<Iterator>> set;
  qi::rule<Iterator, table(), skipper<Iterator>> table;
  qi::rule<Iterator, record(), skipper<Iterator>> record;

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
