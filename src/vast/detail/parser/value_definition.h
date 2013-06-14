#ifndef VAST_DETAIL_PARSER_VALUE_DEFINITION_H
#define VAST_DETAIL_PARSER_VALUE_DEFINITION_H

#include "vast/detail/parser/value.h"
#include "vast/container.h"
#include "vast/regex.h"

namespace vast {
namespace detail {
namespace parser {

struct vector_inserter
{
  template <typename, typename>
  struct result
  {
    typedef void type;
  };

  template <typename C>
  typename std::enable_if<
    std::is_same<C, vector>::value ||
    std::is_same<C, record>::value
  >::type
  operator()(C& c, vast::value x) const
  {
    c.push_back(std::move(x));
  }

  template <typename C>
  typename std::enable_if<std::is_same<C, set>::value>::type
  operator()(C& c, vast::value x) const
  {
    c.insert(std::move(x));
  }
};

struct table_inserter
{
  template <typename, typename, typename>
  struct result
  {
    typedef void type;
  };

  void operator()(table& t, vast::value k, vast::value v) const
  {
    t.emplace(k, v);
  }
};

template <typename Iterator>
value<Iterator>::value()
  : value::base_type(val)
  , str('"')
  , rx('/')
{
  using boost::phoenix::construct;

  qi::_1_type _1;
  qi::_2_type _2;
  qi::_3_type _3;
  qi::_4_type _4;
  qi::_val_type _val;
  qi::lit_type lit;

  qi::int_type sint;
  qi::uint_type uint;
  qi::real_parser<double, qi::strict_real_policies<double>> strict_double;

  boost::phoenix::function<vector_inserter> vector_insert;
  boost::phoenix::function<table_inserter> table_insert;

  val
    =   time_pt               [_val = construct<vast::value>(_1)]
    |   time_dur              [_val = construct<vast::value>(_1)]
    |   (addr >> '/' >> uint) [_val = construct<prefix>(_1, _2)]
    |   prt                   [_val = construct<vast::value>(_1)]
    |   addr                  [_val = construct<vast::value>(_1)]
    |   strict_double         [_val = construct<vast::value>(_1)]
    |   uint                  [_val = construct<vast::value>(_1)]
    |   sint                  [_val = construct<vast::value>(_1)]
    |   vector                [_val = construct<vast::value>(_1)]
    |   table                 [_val = construct<vast::value>(_1)]
    |   set                   [_val = construct<vast::value>(_1)]
    |   record                [_val = construct<vast::value>(_1)]
    |   lit("T")              [_val = construct<vast::value>(true)]
    |   lit("F")              [_val = construct<vast::value>(false)]
    |   rx                    [_val = construct<regex>(_1)]
    |   str                   [_val = construct<vast::value>(_1)]
    ;

  vector
    =   '[' 
    >>  val [vector_insert(_val, _1)] % ',' 
    >>  ']'
    ;

  table
    =   '{' 
    >>  (   val
        >>  "->" 
        >>  val
        )   [table_insert(_val, _1, _2)] % ','
    >>  '}'
    ;

  set
    =   '{' 
    >>  val [vector_insert(_val, _1)] % ',' 
    >>  '}'
    ;

  record
    =   '(' 
    >>  val [vector_insert(_val, _1)] % ',' 
    >>  ')'
    ;

}

} // namespace parser
} // namespace detail
} // namespace vast

#endif
