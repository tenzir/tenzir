/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#ifndef VAST_CONCEPT_PARSEABLE_VAST_HTTP_HPP
#define VAST_CONCEPT_PARSEABLE_VAST_HTTP_HPP

#include <algorithm>
#include <cctype>
#include <string>

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/string.hpp"
#include "vast/concept/parseable/numeric/real.hpp"
#include "vast/concept/parseable/vast/uri.hpp"
#include "vast/http.hpp"
#include "vast/uri.hpp"

namespace vast {

struct http_header_parser : parser<http_header_parser> {
  using attribute = http::header;

  static auto make() {
    auto to_upper = [](std::string name) {
      std::transform(name.begin(), name.end(), name.begin(), ::toupper);
      return name;
    };
    using namespace parsers;
    auto name = +(printable - ':') ->* to_upper;
    auto value = +printable;
    auto ws = *' '_p;
    return name >> ':' >> ws >> value;
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const {
    static auto p = make();
    return p(f, l, unused);
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, http::header& a) const {
    static auto p = make();
    a.name.clear();
    a.value.clear();
    return p(f, l, a.name, a.value);
  }
};

template <>
struct parser_registry<http::header> {
  using type = http_header_parser;
};

struct http_request_parser : parser<http_request_parser> {
  using attribute = http::request;

  static auto make() {
    using namespace parsers;
    auto crlf = "\r\n";
    auto word = +(printable - ' ');
    auto method = word;
    auto uri = make_parser<vast::uri>();
    auto proto = +alpha;
    auto version = parsers::real;
    auto header = make_parser<http::header>() >> crlf;
    auto body = *printable;
    auto request
      =   method >> ' ' >> uri >> ' ' >> proto >> '/' >> version >> crlf
      >>  *header >> crlf
      >>  body;
      ;
    return request;
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const {
    static auto p = make();
    return p(f, l, unused);
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, http::request& a) const {
    static auto p = make();
    return p(f, l, a.method, a.uri, a.protocol, a.version, a.headers, a.body);
  }
};

template <>
struct parser_registry<http::request> {
  using type = http_request_parser;
};

} // namespace vast

#endif
