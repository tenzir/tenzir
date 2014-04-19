#ifndef VAST_DETAIL_PARSER_QUERY_DEFINITION_H
#define VAST_DETAIL_PARSER_QUERY_DEFINITION_H

#include "vast/detail/parser/query.h"

namespace vast {
namespace detail {
namespace parser {

template <typename Iterator>
query<Iterator>::query(error_handler<Iterator>& on_error)
  : query::base_type{start},
    value_expr{on_error}
{
  qi::_1_type _1;
  qi::_2_type _2;
  qi::_3_type _3;
  qi::_4_type _4;
  qi::raw_type raw;
  qi::lexeme_type lexeme;
  qi::repeat_type repeat;
  qi::alpha_type alpha;
  qi::alnum_type alnum;
  qi::ulong_type ulong;

  boolean_op.add
    ("||", logical_or)
    ("&&", logical_and)
    ;

  pred_op.add
    ("~",   match)
    ("!~",  not_match)
    ("==",  equal)
    ("!=",  not_equal)
    ("<",   less)
    ("<=",  less_equal)
    (">",   greater)
    (">=",  greater_equal)
    ("in",  in)
    ("!in", not_in)
    ("ni",  ni)
    ("!ni", not_ni)
    ("[+",  in)
    ("[-",  not_in)
    ("+]",  ni)
    ("-]",  not_ni)
    ;

  type.add
    ("bool",      bool_value)
    ("int",       int_value)
    ("count",     uint_value)
    ("double",    double_value)
    ("interval",  time_range_value)
    ("time",      time_point_value)
    ("string",    string_value)
    ("record",    record_value)
    ("vector",    record_value)
    ("set",       record_value)
    ("table",     table_value)
    ("addr",      address_value)
    ("subnet",    prefix_value)
    ("port",      port_value)
    ;

  start
    =   group >> *(boolean_op > group)
    ;

  group
    =   '(' >> start >> ')'
    |   pred
    ;

  pred
    =   tag_pred
    |   type_pred
    |   offset_pred
    |   schema_pred
    |   ('!' > not_pred)
    ;

  tag_pred
    =   '&'
    >   identifier
    >   pred_op
    >   value_expr
    ;

  type_pred
    =   ':'
    >   type
    >   pred_op
    >   value_expr
    ;

  offset_pred
    =   identifier
    >>  '@'
    >   ulong % ','
    >   pred_op
    >   value_expr
    ;

  schema_pred
    =   glob >> *('.' > glob)
    >   pred_op
    >   value_expr
    ;

  not_pred
    =   pred
    ;

  identifier
    =   raw[lexeme[(alpha | '_') >> *(alnum | '_' )]]
    ;

  glob
    =   raw[lexeme[(alpha | '_' | '*' | '?') >> *(alnum | '_' | '*' | '?')]]
    ;

  event_name
    =   raw[lexeme[ ((alpha | '_') >> *(alnum | '_' )) % repeat(2)[':'] ]]
    ;

  BOOST_SPIRIT_DEBUG_NODES(
      (start)
      (pred)
      (tag_pred)
      (type_pred)
      (offset_pred)
      (schema_pred)
      (identifier)
      );

  on_error.set(start, _4, _3);

  boolean_op.name("binary boolean operator");
  pred_op.name("predicate operator");
  type.name("type");
  start.name("query");
  pred.name("predicate");
  tag_pred.name("tag predicate");
  offset_pred.name("offset predicate");
  type_pred.name("type predicate");
  schema_pred.name("schema predicate");
  not_pred.name("negated predicate");
  identifier.name("identifier");
  glob.name("glob expression");
}

} // namespace ast
} // namespace detail
} // namespace vast

#endif
