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
    ;

  type.add
    ("bool",      bool_type)
    ("int",       int_type)
    ("count",     uint_type)
    ("double",    double_type)
    ("duration",  time_range_type)
    ("time",      time_point_type)
    ("string",    string_type)
    ("record",    record_type)
    ("vector",    record_type)
    ("set",       record_type)
    ("table",     table_type)
    ("addr",      address_type)
    ("prefix",    prefix_type)
    ("port",      port_type)
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
    |   event_pred
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
    =   '@'
    >   ulong % ','
    >   pred_op
    >   value_expr
    ;

  event_pred
    =   glob >> *('$' > identifier)
    >   pred_op
    >   value_expr
    ;

  not_pred
    =   pred
    ;

  identifier
    =   raw[lexeme[(alpha | '_') >> *(alnum | '_' )]]
    ;

  // Supports currently only one level of scoping.
  glob
    = raw
      [
        lexeme
        [
              (alpha | '_' | '*' | '?')
          >> *(alnum | '_' | '*' | '?')
          >> -(   repeat(2)[':']
              >   (alpha | '_' | '*' | '?')
              >> *(alnum | '_' | '*' | '?')
              )
         ]
      ]
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
      (event_pred)
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
  event_pred.name("event predicate");
  not_pred.name("negated predicate");
  identifier.name("identifier");
}

} // namespace ast
} // namespace detail
} // namespace vast

#endif
