#ifndef VAST_CONCEPT_PARSEABLE_VAST_HTTP_H
#define VAST_CONCEPT_PARSEABLE_VAST_HTTP_H

#include <algorithm>
#include <cctype>
#include <string>

#include "vast/concept/parseable/core.h"
#include "vast/concept/parseable/string.h"
#include "vast/concept/parseable/numeric/real.h"
#include "vast/concept/parseable/vast/uri.h"
#include "vast/http.h"
#include "vast/uri.h"
#include "vast/util/string.h"

namespace vast {

struct http_header_parser : parser<http_header_parser> {
  using attribute = http::header;

  static auto make() {
    auto to_upper = [](std::string name) {
      std::transform(name.begin(), name.end(), name.begin(), ::toupper);
      return name;
    };
    using namespace parsers;
    auto name = +(parsers::print - ':') ->* to_upper;
    auto value = +parsers::print;
    auto ws = *lit(' ');
    return name >> ':' >> ws >> value;
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const {
    static auto p = make();
    return p.parse(f, l, unused);
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, http::header& a) const {
    static auto p = make();
    a.name.clear();
    a.value.clear();
    auto t = std::tie(a.name, a.value);
    return p.parse(f, l, t);
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
    auto word = +(parsers::print - ' ');
    auto method = word;
    auto uri = make_parser<vast::uri>();
    auto proto = +alpha;
    auto version = real;
    auto header = make_parser<http::header>() >> crlf;
    auto body = *parsers::print;
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
    return p.parse(f, l, unused);
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, http::request& a) const {
    static auto p = make();
    auto t =
      std::tie(a.method, a.uri, a.protocol, a.version, a.headers, a.body);
    return p.parse(f, l, t);
  }
};

template <>
struct parser_registry<http::request> {
  using type = http_request_parser;
};

} // namespace vast

#endif
