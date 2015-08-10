#ifndef VAST_CONCEPT_PARSEABLE_VAST_DETAIL_ADDRESS_H
#define VAST_CONCEPT_PARSEABLE_VAST_DETAIL_ADDRESS_H

#include "vast/die.h"
#include "vast/concept/parseable/vast/detail/boost.h"
#include "vast/concept/parseable/vast/address.h"

#ifdef VAST_CLANG
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunsequenced"
#endif

namespace vast {
namespace detail {
namespace parser {

namespace qi = boost::spirit::qi;

namespace detail {

struct address_maker {
  template <typename, typename>
  struct result {
    typedef void type;
  };

  void operator()(vast::address& a, std::string const& str) const {
    if (!parsers::addr(str, a))
      die("parser implementation mismatch");
  }
};

} // namespace detail

template <typename Iterator>
struct address : qi::grammar<Iterator, vast::address()> {
  address() : address::base_type(addr) {
    using boost::phoenix::construct;
    using boost::phoenix::begin;
    using boost::phoenix::end;

    qi::_1_type _1;
    qi::_val_type _val;
    qi::lit_type lit;
    qi::raw_type raw;
    qi::repeat_type rep;

    qi::char_type chr;
    qi::digit_type digit;
    qi::xdigit_type xdigit;

    boost::phoenix::function<detail::address_maker> make_addr;

    addr
      =   addr_str [make_addr(_val, _1)]
      ;

    addr_str
      =   raw[addr_v4] [_val = construct<std::string>(begin(_1), end(_1))]
      |   raw[addr_v6] [_val = construct<std::string>(begin(_1), end(_1))]
      ;

    addr_v6
      =                                             rep(6)[h16 >> ':'] >> ls32
      |                                     "::" >> rep(5)[h16 >> ':'] >> ls32
      |   -(                        h16) >> "::" >> rep(4)[h16 >> ':'] >> ls32
      |   -(rep(0, 1)[h16_colon] >> h16) >> "::" >> rep(3)[h16 >> ':'] >> ls32
      |   -(rep(0, 2)[h16_colon] >> h16) >> "::" >> rep(2)[h16 >> ':'] >> ls32
      |   -(rep(0, 3)[h16_colon] >> h16) >> "::" >>        h16 >> ':'  >> ls32
      |   -(rep(0, 4)[h16_colon] >> h16) >> "::"                       >> ls32
      |   -(rep(0, 5)[h16_colon] >> h16) >> "::"                       >> h16
      |   -(rep(0, 6)[h16_colon] >> h16) >> "::"
      ;

    // Matches a 1-4 hex digits followed by a *single* colon. If we did not have
    // this rule, the input "f00::" would not be detected correctly, since a
    // rule of the form
    //
    //    -(rep(0, *)[h16 >> ':'] >> h16) >> "::"
    //
    // already consumes the input "f00:" after the first repitition parser, thus
    // erroneously leaving only ":" for next rule `>> h16` to consume.
    h16_colon
      =   h16 >> ':' >> !lit(':')
      ;

    h16
      =   rep(1, 4)[xdigit]
      ;

    ls32
      =   h16 >> ':' >> h16
      |   addr_v4
      ;

    addr_v4
      =   dec >> '.' >> dec >> '.' >> dec >> '.' >> dec
      ;

    dec
      =   "25" >> chr("0-5")
      |   '2' >> chr("0-4") >> digit
      |   '1' >> rep(2)[digit]
      |   chr("1-9") >> digit
      |   digit
      ;

    BOOST_SPIRIT_DEBUG_NODES(
        (addr_v6)
        (h16)
        (ls32)
        (addr_v4)
    );
  }

  qi::rule<Iterator, vast::address()> addr;
  qi::rule<Iterator, std::string()> addr_str;
  qi::rule<Iterator> addr_v6, h16, h16_colon, ls32, addr_v4, dec;
};

} // namespace parser
} // namespace detail
} // namespace vast

#ifdef VAST_CLANG
#pragma clang diagnostic pop
#endif

#endif
