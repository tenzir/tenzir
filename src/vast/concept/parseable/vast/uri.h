#ifndef VAST_CONCEPT_PARSEABLE_VAST_URI_H
#define VAST_CONCEPT_PARSEABLE_VAST_URI_H

#include <algorithm>
#include <cctype>
#include <string>

#include "vast/concept/parseable/core.h"
#include "vast/concept/parseable/string.h"
#include "vast/concept/parseable/numeric/real.h"
#include "vast/uri.h"
#include "vast/util/string.h"

namespace vast {

// A URI parser based on RFC 3986.
struct uri_parser : parser<uri_parser> {
  using attribute = uri;

  static auto make() {
    using namespace parsers;
    auto percent_unescape = [](std::string str) {
      return util::percent_unescape(str);
    };
    auto protocol_ignore_char = ':'_p | '/';
    auto protocol = *(printable - protocol_ignore_char);
    auto hostname = *(printable - protocol_ignore_char);
    auto port = u16;
    auto path_ignore_char = '/'_p | '?' | '#' | ' ';
    auto path_segment = *(printable - path_ignore_char) ->* percent_unescape;
    auto option_key = +(printable - '=') ->* percent_unescape;
    auto option_ignore_char = '&'_p | '#' | ' ';
    auto option_value = +(printable - option_ignore_char) ->* percent_unescape;
    auto option = option_key >> '=' >> option_value;
    auto fragment = *(printable - ' ');
    auto uri
      =  ~(protocol >> ':')
      >> ~("//" >> hostname)
      >> ~(':' >> port)
      >> '/' >> path_segment % '/'
      >> ~('?' >> option % '&')
      >> ~('#' >>  fragment)
      ;
    return uri;
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const {
    static auto p = make();
    return p.parse(f, l, unused);
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, uri& u) const {
    static auto p = make();
    auto t = std::tie(u.protocol, u.hostname, u.port, u.path, u.options,
                      u.fragment);
    return p.parse(f, l, t);
  }
};

template <>
struct parser_registry<uri> {
  using type = uri_parser;
};

} // namespace vast

#endif
