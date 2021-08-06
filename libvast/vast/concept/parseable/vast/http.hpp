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
#include "vast/concept/parseable/vast/uri.hpp"
#include "vast/http.hpp"
#include "vast/uri.hpp"

#include <algorithm>
#include <cctype>
#include <string>

namespace vast {

struct http_header_parser : parser_base<http_header_parser> {
  using attribute = http::header;

  static auto make() {
    using namespace parser_literals;
    auto to_upper = [](std::string name) {
      std::transform(name.begin(), name.end(), name.begin(), ::toupper);
      return name;
    };
    auto name = +(parsers::printable - ':')->*to_upper;
    auto value = +parsers::printable;
    auto ws = *' '_p;
    return name >> ':' >> ws >> value;
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    static auto p = make();
    return p(f, l, unused);
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, http::header& a) const {
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

struct http_request_parser : parser_base<http_request_parser> {
  using attribute = http::request;

  static auto make() {
    using namespace parsers;
    auto crlf = "\r\n";
    auto word = +(parsers::printable - ' ');
    auto method = word;
    auto uri = make_parser<vast::uri>();
    auto proto = +alpha;
    auto version = parsers::real;
    auto header = make_parser<http::header>() >> crlf;
    auto body = *parsers::printable;
    auto request
      =   method >> ' ' >> uri >> ' ' >> proto >> '/' >> version >> crlf
      >>  *header >> crlf
      >>  body;
      ;
    return request;
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    static auto p = make();
    return p(f, l, unused);
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, http::request& a) const {
    static auto p = make();
    return p(f, l, a.method, a.uri, a.protocol, a.version, a.headers, a.body);
  }
};

template <>
struct parser_registry<http::request> {
  using type = http_request_parser;
};

} // namespace vast

