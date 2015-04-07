#ifndef VAST_CONCEPT_PARSEABLE_VAST_DETAIL_ADDRESS_H
#define VAST_CONCEPT_PARSEABLE_VAST_DETAIL_ADDRESS_H

#include <arpa/inet.h>  // inet_pton
#include <sys/socket.h> // AF_INET*

#define BOOST_SPIRIT_NO_PREDEFINED_TERMINALS
#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix.hpp>

#include "vast/access.h"
#include "vast/address.h"
#include "vast/concept/parseable/core/parser.h"

namespace vast {
namespace detail {

namespace qi = boost::spirit::qi;

template <typename Iterator>
struct ip_address_parser : qi::grammar<Iterator, int()>
{
  ip_address_parser()
    : ip_address_parser::base_type(addr)
  {
    qi::lit_type lit;
    qi::repeat_type rep;
    qi::char_type chr;
    qi::digit_type digit;
    qi::xdigit_type xdigit;
    qi::_val_type _val;

    addr
      =   v4 [_val = 4]
      |   v6 [_val = 6]
      ;

    v6
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
      |   v4
      ;

    v4
      =   dec >> '.' >> dec >> '.' >> dec >> '.' >> dec
      ;

    dec
      =   "25" >> chr("0-5")
      |   '2' >> chr("0-4") >> digit
      |   '1' >> rep(2)[digit]
      |   chr("1-9") >> digit
      |   digit
      ;
  }

  qi::rule<Iterator, int()> addr;
  qi::rule<Iterator> v4, v6, h16, h16_colon, ls32, dec;
};

} // namespace detail

template <>
struct access::parser<address> : vast::parser<access::parser<address>>
{
  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, unused_type) const
  {
    static detail::ip_address_parser<Iterator> grammar;
    return boost::spirit::qi::parse(f, l, grammar);
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const
  {
    // Determine the IP address with the Boost spirit grammar.
    static detail::ip_address_parser<Iterator> grammar;
    auto version = -1;
    auto save = f;
    if (! boost::spirit::qi::parse(f, l, grammar, version))
      return false;
    // Parse actual address.
    std::string str{save, l}; // TODO: elide copy when possible (N4284).
    if (version == 6)
      return ::inet_pton(AF_INET6, str.data(), &a.bytes_);
    std::copy(address::v4_mapped_prefix.begin(),
              address::v4_mapped_prefix.end(),
              a.bytes_.begin());
    return ::inet_pton(AF_INET, str.data(), &a.bytes_[12]);
  }

  // This overload elides an extra std::string copy when working with string
  // iterators. What we really want is dispatch based on the notion of
  // *contiguuous iterators* (N4284). But that's not yet available yet.
  template <typename Attribute>
  bool parse(std::string::const_iterator& f,
             std::string::const_iterator const& l, Attribute& a) const
  {
    static detail::ip_address_parser<std::string::const_iterator> grammar;
    auto version = -1;
    auto save = f;
    if (! boost::spirit::qi::parse(f, l, grammar, version))
      return false;
    if (version == 6)
      return ::inet_pton(AF_INET6, &*save, &a.bytes_);
    std::copy(address::v4_mapped_prefix.begin(),
              address::v4_mapped_prefix.end(),
              a.bytes_.begin());
    return ::inet_pton(AF_INET, &*save, &a.bytes_[12]);
  }
};

template <>
struct parser_registry<address>
{
  using type = access::parser<address>;
};

} // namespace vast

#endif
