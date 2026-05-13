//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core.hpp"
#include "tenzir/concept/parseable/numeric/integral.hpp"
#include "tenzir/concept/parseable/string/char.hpp"
#include "tenzir/concept/parseable/string/char_class.hpp"
#include "tenzir/concept/parseable/tenzir/ip.hpp"
#include "tenzir/concept/parseable/tenzir/port.hpp"
#include "tenzir/endpoint.hpp"

#include <optional>
#include <string>
#include <tuple>
#include <utility>

namespace tenzir {

struct EndpointParser : parser_base<EndpointParser> {
  using attribute = Endpoint;

  template <class Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const {
    auto result = Endpoint{};
    return parse(f, l, result);
  }

  static auto make() {
    using namespace parsers;
    using namespace parser_literals;
    auto endpoint_port = parsers::port | u16.then([](port::number_type number) {
      return tenzir::port{number};
    });
    auto host_char = alnum | ch<'-'> | ch<'_'> | ch<'.'>;
    auto host = +host_char;
    auto ipv6_char = xdigit | ch<':'> | ch<'.'>;
    auto ipv6_host = (+ipv6_char).with([](std::string const& value) {
      return parsers::ipv6(value);
    });
    auto optional_port = -(':'_p >> endpoint_port);
    auto to_endpoint = [](std::string host, std::optional<tenzir::port> port) {
      auto result = Endpoint{};
      result.host = std::move(host);
      if (port) {
        result.port = *port;
      }
      return result;
    };
    auto bracketed_ipv6
      = ('['_p >> ipv6_host >> ']'_p >> optional_port)
          .then([=](std::tuple<std::string, std::optional<tenzir::port>> x) {
            return to_endpoint(std::move(std::get<0>(x)),
                               std::move(std::get<1>(x)));
          });
    auto bare_ipv6 = ipv6_host.then([](std::string host) {
      auto result = Endpoint{};
      result.host = std::move(host);
      return result;
    });
    auto host_and_port = (*host_char >> ':'_p >> endpoint_port)
                           .then([](std::tuple<std::string, tenzir::port> x) {
                             auto result = Endpoint{};
                             result.host = std::move(std::get<0>(x));
                             result.port = std::get<1>(x);
                             return result;
                           });
    auto host_only = host.then([](std::string host) {
      auto result = Endpoint{};
      result.host = std::move(host);
      return result;
    });
    return bracketed_ipv6 | bare_ipv6 | host_and_port | host_only;
  }

  template <class Iterator>
  bool parse(Iterator& f, Iterator const& l, Endpoint& e) const {
    static auto const p = make();
    return p(f, l, e);
  }
};

template <>
struct parser_registry<Endpoint> {
  using type = EndpointParser;
};

namespace parsers {

auto const endpoint = make_parser<tenzir::Endpoint>();

} // namespace parsers
} // namespace tenzir
