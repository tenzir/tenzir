#ifndef VAST_QUERY_PARSER_SKIPPER_H
#define VAST_QUERY_PARSER_SKIPPER_H

#include <boost/spirit/include/qi.hpp>

namespace vast {
namespace query {
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
            ;
    }

    qi::rule<Iterator> start;
};

} // namespace ast
} // namespace query
} // namespace vast

#endif
