#ifndef VAST_CONCEPT_PARSEABLE_VAST_DETAIL_DATA_H
#define VAST_CONCEPT_PARSEABLE_VAST_DETAIL_DATA_H

#include "vast/data.h"

#include "vast/concept/parseable/vast/detail/escaped_string.h"
#include "vast/concept/parseable/vast/detail/address.h"
#include "vast/concept/parseable/vast/detail/port.h"
#include "vast/concept/parseable/vast/detail/skipper.h"
#include "vast/concept/parseable/vast/detail/time_duration.h"
#include "vast/concept/parseable/vast/detail/time_point.h"

#ifdef VAST_CLANG
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunsequenced"
#endif

namespace vast {
namespace detail {
namespace parser {

struct sequence_inserter {
  template <typename, typename>
  struct result {
    using type = void;
  };

  template <typename Vector, typename T>
  void operator()(Vector& v, T&& x) const {
    v.push_back(std::forward<T>(x));
  }

  template <typename T>
  void operator()(set& s, T&& x) const {
    s.insert(std::forward<T>(x));
  }
};

struct map_inserter {
  template <typename, typename, typename>
  struct result {
    using type = void;
  };

  template <typename K, typename V>
  void operator()(table& t, K&& k, V&& v) const {
    t.emplace(std::forward<K>(k), std::forward<V>(v));
  }
};

struct data_factory {
  template <typename>
  struct result {
    using type = vast::data;
  };

  template <typename T>
  vast::data operator()(T&& x) const {
    return {std::forward<T>(x)};
  }
};

template <typename Iterator>
struct data : qi::grammar<Iterator, vast::data(), skipper<Iterator>> {
  data() : data::base_type(dta), str('"'), pat('/') {
    using boost::phoenix::construct;

    qi::_1_type _1;
    qi::_2_type _2;
    qi::_val_type _val;
    qi::lit_type lit;

    qi::int_type sint;
    qi::uint_type uint;
    qi::real_parser<double, qi::strict_real_policies<double>> strict_double;

    boost::phoenix::function<sequence_inserter> vector_insert, set_insert;
    boost::phoenix::function<map_inserter> map_insert;
    boost::phoenix::function<data_factory> make_data;

    dta
      =   time_pt               [_val = _1]
      |   time_dur              [_val = _1]
      |   (addr >> '/' >> uint) [_val = construct<subnet>(_1, _2)]
      |   prt                   [_val = _1]
      |   addr                  [_val = _1]
      |   strict_double         [_val = _1]
      |   uint                  [_val = _1]
      |   sint                  [_val = _1]
      |   vec                   [_val = _1]
      |   tbl                   [_val = _1]
      |   st                    [_val = _1]
      |   rec                   [_val = _1]
      |   str                   [_val = _1]
      |   pat                   [_val = construct<pattern>(_1)]
      |   lit("T")              [_val = construct<vast::data>(true)]
      |   lit("F")              [_val = construct<vast::data>(false)]
      |   lit("nil")
      ;

    rec
      =   '('
      >>  dta [vector_insert(_val, _1)] % ','
      >>  ')'
      ;

    vec
      =   '['
      >>  dta [vector_insert(_val, _1)] % ','
      >>  ']'
      ;

    st 
      =   '{'
      >>  dta [set_insert(_val, _1)] % ','
      >>  '}'
      ;

    tbl
      =   '{'
      >>  (   dta
          >>  "->"
          >>  dta
          )   [map_insert(_val, _1, _2)] % ','
      >>  '}'
      ;
  }

  qi::rule<Iterator, vast::data(), skipper<Iterator>> dta;
  qi::rule<Iterator, record(), skipper<Iterator>> rec;
  qi::rule<Iterator, vector(), skipper<Iterator>> vec;
  qi::rule<Iterator, set(), skipper<Iterator>> st;
  qi::rule<Iterator, table(), skipper<Iterator>> tbl;

  time_duration<Iterator> time_dur;
  time_point<Iterator> time_pt;
  address<Iterator> addr;
  port<Iterator> prt;
  escaped_string<Iterator> str, pat;
};

} // namespace parser
} // namespace detail
} // namespace vast

#ifdef VAST_CLANG
#pragma clang diagnostic pop
#endif

#endif
