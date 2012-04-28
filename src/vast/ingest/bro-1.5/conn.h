#ifndef VAST_INGEST_BRO_1_5_CONN_H
#define VAST_INGEST_BRO_1_5_CONN_H

// This grammar has more struct fields than the default can handle.
#define FUSION_MAX_VECTOR_SIZE 20

// Improves compile times significantly at the cost of predefining terminals.
#define BOOST_SPIRIT_NO_PREDEFINED_TERMINALS

#include <ze/parser/address.h>
#include "vast/ingest/bro-1.5/ast.h"

namespace vast {
namespace ingest {
namespace bro15 {
namespace parser {

namespace qi = boost::spirit::qi;
namespace ascii = qi::ascii;

template <typename Iterator>
struct skipper : qi::grammar<Iterator>
{
    skipper()
      : skipper::base_type(start)
    {
        ascii::space_type space;

        start =
                space - '\n'    // Tab & space
            ;
    };

    qi::rule<Iterator> start;
};

template <typename Iterator>
struct connection : qi::grammar<Iterator, ast::conn(), skipper<Iterator>>
{
    connection();

    qi::rule<Iterator, ast::conn(), skipper<Iterator>> conn;
    qi::rule<Iterator, std::string()> id;
    qi::rule<Iterator, std::string()> addl;

    ze::parser::address<Iterator> addr;
};

} // namespace parser
} // namespace bro15
} // namespace ingest
} // namespace vast

#endif
