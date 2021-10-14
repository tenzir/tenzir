//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/access.hpp"
#include "vast/address.hpp"
#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/string/char_class.hpp"
#include "vast/detail/assert.hpp"

#include <arpa/inet.h>  // inet_pton
#include <sys/socket.h> // AF_INET*

#include <cstring>

namespace vast {

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
struct ip_address_parser : vast::parser_base<ip_address_parser> {
  using attribute = address;

  static auto make_v4() {
    using namespace parsers;
    auto dec
      = integral_parser<uint16_t, 3, 1>{}.with([](auto i) { return i < 256; });
      ;
    auto v4
      = dec >> '.' >> dec >> '.' >> dec >> '.' >> dec
      ;
    return v4;
  }

  static auto make_v6() {
    using namespace parsers;
    using namespace parser_literals;
    auto h16
      = rep<1, 4>(xdigit)
      ;
    // Matches a 1-4 hex digits followed by a *single* colon. If we did not have
    // this parser, the input "f00::" would not be detected correctly, since a
    // rule of the form
    //
    //    -(repeat<0, *>{h16 >> ':'} >> h16) >> "::"
    //
    // already consumes the input "f00:" after the first repitition parser, thus
    // erroneously leaving only ":" for next rule `>> h16` to consume.
    auto h16_colon
      = h16 >> ':' >> !':'_p
      ;
    auto ls32
      = h16 >> ':' >> h16
      | make_v4();
      ;
    auto v6
      =                                             rep<6>(h16 >> ':') >> ls32
      |                                     "::" >> rep<5>(h16 >> ':') >> ls32
      |   -(                        h16) >> "::" >> rep<4>(h16 >> ':') >> ls32
      |   -(rep<0, 1>(h16_colon) >> h16) >> "::" >> rep<3>(h16 >> ':') >> ls32
      |   -(rep<0, 2>(h16_colon) >> h16) >> "::" >> rep<2>(h16 >> ':') >> ls32
      |   -(rep<0, 3>(h16_colon) >> h16) >> "::" >>        h16 >> ':'  >> ls32
      |   -(rep<0, 4>(h16_colon) >> h16) >> "::"                       >> ls32
      |   -(rep<0, 5>(h16_colon) >> h16) >> "::"                       >> h16
      |   -(rep<0, 6>(h16_colon) >> h16) >> "::"
      ;
    return v6;
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    static auto v4 = make_v4();
    static auto v6 = make_v6();
    if (v4(f, l, unused))
      return true;
    if (v6(f, l, unused))
      return true;
    return false;
  }
};

struct address_parser : vast::parser_base<address_parser> {
  using attribute = address;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    static auto const p = ip_address_parser{};
    return p(f, l, unused);
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, address& a) const {
    static auto const v4 = ip_address_parser::make_v4();
    static auto const v6 = ip_address_parser::make_v6();
    std::array<uint8_t, 16> bytes;
    auto begin = f;
    if (v4(f, l, bytes[12], bytes[13], bytes[14], bytes[15])) {
      a = address::v4(std::span<const uint8_t, 4>{bytes.data() + 12, 4});
      return true;
    }
    if (v6(f, l, unused)) {
      // We still need to enhance the parseable concept with a few more tools
      // so that we can transparently parse into 16-byte sequence. Until
      // then, we rely on inet_pton. Unfortunately this incurs an extra copy
      // because inet_pton needs a NUL-terminated string of the address *only*,
      // i.e., it does not work when other characters follow the address.
      char buf[INET6_ADDRSTRLEN];
      std::memset(buf, 0, sizeof(buf));
      VAST_ASSERT(f - begin < INET6_ADDRSTRLEN);
      std::copy(begin, f, buf);
      auto okay = ::inet_pton(AF_INET6, buf, bytes.data()) == 1;
      if (okay)
        a = address{bytes};
      return okay;
    }
    return false;
  }
};

template <>
struct parser_registry<address> {
  using type = address_parser;
};

namespace parsers {

static auto const addr = make_parser<vast::address>();

} // namespace parsers

} // namespace vast
