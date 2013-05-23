#ifndef VAST_DETAIL_PARSER_ADDRESS_DEFINITION_H
#define VAST_DETAIL_PARSER_ADDRESS_DEFINITION_H

#include "vast/detail/parser/address.h"

namespace vast {
namespace detail {
namespace parser {

/// An IP address parser which accepts addresses according to [SIP IPv6
/// ABNF](http://tools.ietf.org/html/draft-ietf-sip-ipv6-abnf-fix-05).
/// This IETF draft defines the grammar as follows:
///
///     IPv6address   =                             6( h16 ":" ) ls32
///                    /                       "::" 5( h16 ":" ) ls32
///                    / [               h16 ] "::" 4( h16 ":" ) ls32
///                    / [ *1( h16 ":" ) h16 ] "::" 3( h16 ":" ) ls32
///                    / [ *2( h16 ":" ) h16 ] "::" 2( h16 ":" ) ls32
///                    / [ *3( h16 ":" ) h16 ] "::"    h16 ":"   ls32
///                    / [ *4( h16 ":" ) h16 ] "::"              ls32
///                    / [ *5( h16 ":" ) h16 ] "::"              h16
///                    / [ *6( h16 ":" ) h16 ] "::"
///
///      h16           = 1*4HEXDIG
///      ls32          = ( h16 ":" h16 ) / IPv4address
///      IPv4address   = dec-octet "." dec-octet "." dec-octet "." dec-octet
///      dec-octet     = DIGIT                 ; 0-9
///                    / %x31-39 DIGIT         ; 10-99
///                    / "1" 2DIGIT            ; 100-199
///                    / "2" %x30-34 DIGIT     ; 200-249
///                    / "25" %x30-35          ; 250-255
template <typename Iterator>
address<Iterator>::address()
  : address::base_type(addr)
{
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

  addr
    =   addr_str [_val = construct<vast::address>(_1)]
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

} // namespace parser
} // namespace detail
} // namespace vast

#endif
