#ifndef VAST_DETAIL_PARSER_ADDRESS_H
#define VAST_DETAIL_PARSER_ADDRESS_H

#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix.hpp>
#include "vast/address.h"

namespace vast {
namespace detail {
namespace parser {

namespace qi = boost::spirit::qi;

template <typename Iterator>
struct address : qi::grammar<Iterator, vast::address()>
{
  address();

  qi::rule<Iterator, vast::address()> addr;
  qi::rule<Iterator, std::string()> addr_str;
  qi::rule<Iterator> addr_v6, h16, h16_colon, ls32, addr_v4, dec;
};

} // namespace parser
} // namespace detail
} // namespace vast

#endif
