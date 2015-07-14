#ifndef VAST_CONCEPT_PARSEABLE_VAST_DETAIL_PORT_H
#define VAST_CONCEPT_PARSEABLE_VAST_DETAIL_PORT_H

#include "vast/port.h"
#include "vast/concept/parseable/vast/detail/boost.h"

namespace vast {
namespace detail {
namespace parser {

template <typename Iterator>
struct port : qi::grammar<Iterator, vast::port()>
{
  port()
    : port::base_type(p)
  {
    using boost::phoenix::construct;

    qi::_1_type _1;
    qi::_2_type _2;
    qi::_val_type _val;
    qi::lit_type lit;

    qi::uint_type uint;

    p
      =   (uint >> '/' >> proto) [_val = construct<vast::port>(_1, _2)]
      ;

    proto
      =   lit("tcp")     [_val = vast::port::tcp]
      |   lit("udp")     [_val = vast::port::udp]
      |   lit("icmp")    [_val = vast::port::icmp]
      |   lit("?")       [_val = vast::port::unknown]
      |   lit("unknown") [_val = vast::port::unknown]
      ;
  }

  qi::rule<Iterator, vast::port()> p;
  qi::rule<Iterator, vast::port::port_type()> proto;
};

} // namespace parser
} // namespace detail
} // namespace vast

#endif
