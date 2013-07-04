#ifndef VAST_DETAIL_PARSER_SCHEMA_H
#define VAST_DETAIL_PARSER_SCHEMA_H

#include "vast/detail/ast/schema.h"
#include "vast/detail/parser/error_handler.h"
#include "vast/detail/parser/skipper.h"

namespace vast {
namespace detail {
namespace parser {

namespace qi = boost::spirit::qi;

template <typename Iterator>
struct schema : qi::grammar<Iterator, ast::schema::schema(), skipper<Iterator>>
{
  schema(error_handler<Iterator>& on_error);

  qi::symbols<char, ast::schema::event_declaration> event_;
  qi::symbols<char, ast::schema::type_info> basic_type_;
  qi::symbols<char, ast::schema::type_info> type_;
  qi::rule<Iterator, ast::schema::schema(), skipper<Iterator>> schema_;
  qi::rule<Iterator, ast::schema::statement(), skipper<Iterator>> statement_;
  qi::rule<Iterator, ast::schema::type_declaration(), skipper<Iterator>> type_decl_;
  qi::rule<Iterator, ast::schema::event_declaration(), skipper<Iterator>> event_decl_;
  qi::rule<Iterator, ast::schema::argument_declaration(), skipper<Iterator>> argument_;
  qi::rule<Iterator, ast::schema::attribute(), skipper<Iterator>> attribute_;
  qi::rule<Iterator, ast::schema::type_type(), skipper<Iterator>> type_type_;
  qi::rule<Iterator, ast::schema::type_info(), skipper<Iterator>> type_info_;
  qi::rule<Iterator, ast::schema::enum_type(), skipper<Iterator>> enum_;
  qi::rule<Iterator, ast::schema::vector_type(), skipper<Iterator>> vector_;
  qi::rule<Iterator, ast::schema::set_type(), skipper<Iterator>> set_;
  qi::rule<Iterator, ast::schema::table_type(), skipper<Iterator>> table_;
  qi::rule<Iterator, ast::schema::record_type(), skipper<Iterator>> record_;
  qi::rule<Iterator, std::string()> identifier_;
};

} // namespace parser
} // namespace detail
} // namespace vast

#endif
