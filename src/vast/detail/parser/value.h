#ifndef VAST_DETAIL_PARSER_VALUE_H
#define VAST_DETAIL_PARSER_VALUE_H

#include "vast/detail/parser/escaped_string.h"
#include "vast/detail/parser/address.h"
#include "vast/detail/parser/duration.h"
#include "vast/detail/parser/port.h"
#include "vast/detail/parser/skipper.h"
#include "vast/detail/parser/time_point.h"
#include "vast/value.h"

#ifdef VAST_CLANG
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunsequenced"
#endif

namespace vast {
namespace detail {
namespace parser {

struct container_inserter
{
  template <typename, typename>
  struct result
  {
    typedef void type;
  };

  template <typename Container>
  void operator()(Container& c, vast::value x) const
  {
    c.push_back(std::move(x));
  }
};

struct map_inserter
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
struct value : qi::grammar<Iterator, vast::value(), skipper<Iterator>>
{
  value()
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

    boost::phoenix::function<container_inserter> container_insert;
    boost::phoenix::function<map_inserter> map_insert;

    val
      =   time_pt               [_val = construct<vast::value>(_1)]
      |   time_dur              [_val = construct<vast::value>(_1)]
      |   (addr >> '/' >> uint) [_val = construct<prefix>(_1, _2)]
      |   prt                   [_val = construct<vast::value>(_1)]
      |   addr                  [_val = construct<vast::value>(_1)]
      |   strict_double         [_val = construct<vast::value>(_1)]
      |   uint                  [_val = construct<vast::value>(_1)]
      |   sint                  [_val = construct<vast::value>(_1)]
      |   vec                   [_val = construct<vast::value>(_1)]
      |   tbl                   [_val = construct<vast::value>(_1)]
      |   st                    [_val = construct<vast::value>(_1)]
      |   rec                   [_val = construct<vast::value>(_1)]
      |   lit("T")              [_val = construct<vast::value>(true)]
      |   lit("F")              [_val = construct<vast::value>(false)]
      |   rx                    [_val = construct<regex>(_1)]
      |   str                   [_val = construct<vast::value>(_1)]
      ;

    rec
      =   '('
      >>  val [container_insert(_val, _1)] % ','
      >>  ')'
      ;

    vec
      =   '['
      >>  val [container_insert(_val, _1)] % ','
      >>  ']'
      ;

    st 
      =   '{'
      >>  val [container_insert(_val, _1)] % ','
      >>  '}'
      ;

    tbl
      =   '{'
      >>  (   val
          >>  "->"
          >>  val
          )   [map_insert(_val, _1, _2)] % ','
      >>  '}'
      ;
  }

  qi::rule<Iterator, vast::value(), skipper<Iterator>> val;
  qi::rule<Iterator, record(), skipper<Iterator>> rec;
  qi::rule<Iterator, vector(), skipper<Iterator>> vec;
  qi::rule<Iterator, set(), skipper<Iterator>> st;
  qi::rule<Iterator, table(), skipper<Iterator>> tbl;

  duration<Iterator> time_dur;
  time_point<Iterator> time_pt;
  address<Iterator> addr;
  port<Iterator> prt;
  escaped_string<Iterator> str, rx;
};

} // namespace parser
} // namespace detail
} // namespace vast

#ifdef VAST_CLANG
#pragma clang diagnostic pop
#endif

#endif
