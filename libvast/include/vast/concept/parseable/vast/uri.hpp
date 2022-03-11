//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric/real.hpp"
#include "vast/concept/parseable/string.hpp"
#include "vast/detail/string.hpp"
#include "vast/uri.hpp"

#include <algorithm>
#include <cctype>
#include <string>

namespace vast {

// A URI parser based on RFC 3986.
struct uri_parser : parser_base<uri_parser> {
  using attribute = uri;

  static auto make() {
    using namespace parsers;
    using namespace parser_literals;
    auto query_unescape = [](std::string str) {
      std::replace(str.begin(), str.end(), '+', ' ');
      return detail::percent_unescape(str);
    };
    auto percent_unescape = [](std::string str) {
      return detail::percent_unescape(str);
    };
    auto scheme_ignore_char = ':'_p | '/';
    auto scheme = *(parsers::printable - scheme_ignore_char);
    auto host = *(parsers::printable - scheme_ignore_char);
    auto port = u16;
    auto path_ignore_char = '/'_p | '?' | '#' | ' ';
    auto path_segment
      = *(parsers::printable - path_ignore_char)->*percent_unescape;
    auto query_key = +(parsers::printable - '=')->*percent_unescape;
    auto query_ignore_char = '&'_p | '#' | ' ';
    auto query_value
      = +(parsers::printable - query_ignore_char)->*query_unescape;
    auto query = query_key >> '=' >> query_value;
    auto fragment = *(parsers::printable - ' ');
    auto uri
      =  ~(scheme >> ':')
      >> ~("//" >> host)
      >> ~(':' >> port)
      >> '/' >> path_segment % '/'
      >> ~('?' >> query % '&')
      >> ~('#' >>  fragment)
      ;
    return uri;
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    static auto p = make();
    return p(f, l, unused);
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, uri& u) const {
    static auto p = make();
    return p(f, l, u.scheme, u.host, u.port, u.path, u.query, u.fragment);
  }
};

template <>
struct parser_registry<uri> {
  using type = uri_parser;
};

} // namespace vast

