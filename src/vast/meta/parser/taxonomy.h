#ifndef VAST_META_PARSER_TAXONOMY_H
#define VAST_META_PARSER_TAXONOMY_H

// Improves compile times significantly at the cost of predefining terminals.
#define BOOST_SPIRIT_NO_PREDEFINED_TERMINALS 

#include <boost/spirit/include/qi.hpp>
#include "vast/meta/ast.h"
#include "vast/util/parser/error_handler.h"
#include "vast/util/parser/skipper.h"

namespace vast {
namespace meta {
namespace parser {

using util::parser::skipper;
namespace qi = boost::spirit::qi;

template <typename Iterator>
struct taxonomy : qi::grammar<Iterator, ast::taxonomy(), skipper<Iterator>>
{
    taxonomy(util::parser::error_handler<Iterator>& error_handler);

    qi::symbols<char, ast::basic_type> basic_type;
    qi::symbols<char, ast::type_info> types;
    qi::symbols<char> events;

    qi::rule<Iterator, ast::taxonomy(), skipper<Iterator>> tax;
    qi::rule<Iterator, ast::statement(), skipper<Iterator>> stmt;
    qi::rule<Iterator, ast::type_declaration(), skipper<Iterator>> type_decl;
    qi::rule<Iterator, ast::event_declaration(), skipper<Iterator>> event_decl;
    qi::rule<Iterator, ast::argument_declaration(), skipper<Iterator>> argument;
    qi::rule<Iterator, ast::attribute(), skipper<Iterator>> attr;
    qi::rule<Iterator, ast::type(), skipper<Iterator>> type;
    qi::rule<Iterator, ast::type_info(), skipper<Iterator>> type_info;
    qi::rule<Iterator, ast::enum_type(), skipper<Iterator>> enum_;
    qi::rule<Iterator, ast::vector_type(), skipper<Iterator>> vector;
    qi::rule<Iterator, ast::set_type(), skipper<Iterator>> set;
    qi::rule<Iterator, ast::table_type(), skipper<Iterator>> table;
    qi::rule<Iterator, ast::record_type(), skipper<Iterator>> record;
    qi::rule<Iterator, std::string()> identifier;
};

} // namespace parser
} // namespace meta
} // namespace vast

#endif
