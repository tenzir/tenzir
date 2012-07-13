#ifndef VAST_UTIL_PARSER_SKIPPER_H
#define VAST_UTIL_PARSER_SKIPPER_H

#include <boost/spirit/include/qi.hpp>

namespace vast {
namespace util {
namespace parser {

namespace qi = boost::spirit::qi;
namespace ascii = boost::spirit::ascii;

template <typename Iterator>
struct skipper : qi::grammar<Iterator>
{
  skipper()
    : skipper::base_type(start)
  {
    qi::char_type char_;
    ascii::space_type space;

    start =
            space                           // Tab, space, CR, LF
        |   "/*" >> *(char_ - "*/") >> "*/" // C-style comments
        |   '#' >> *(char_ - '\n') >> '\n'  // # until end of line
        ;
  }

    qi::rule<Iterator> start;
};

} // namespace ast
} // namespace util
} // namespace vast

#endif
