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
#include "tenzir/concept/parseable/tenzir/ip.hpp"
#include "tenzir/concept/parseable/tenzir/port.hpp"
#include "tenzir/endpoint.hpp"

#include <cctype>
#include <cstdint>
#include <string>

namespace tenzir {

struct EndpointParser : parser_base<EndpointParser> {
  using attribute = Endpoint;

  template <class Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const {
    auto result = Endpoint{};
    return parse(f, l, result);
  }

  template <class Iterator>
  bool parse(Iterator& f, Iterator const& l, Endpoint& e) const {
    using namespace parsers;
    auto first = f;
    if (first == l) {
      return false;
    }
    auto input
      = std::string_view{&*f, static_cast<size_t>(std::distance(f, l))};
    auto parse_port = [](std::string_view value) -> Option<tenzir::port> {
      auto parsed = tenzir::port{};
      if (parsers::port(value, parsed)) {
        return parsed;
      }
      auto number = uint16_t{};
      if (u16(value, number)) {
        return tenzir::port{number};
      }
      return None{};
    };
    auto is_hostname = [](std::string_view value) {
      return std::ranges::all_of(value, [](auto c) {
        return std::isalnum(static_cast<unsigned char>(c)) != 0 or c == '-'
               or c == '_' or c == '.';
      });
    };
    auto result = Endpoint{};
    if (input.front() == '[') {
      auto close = input.find(']');
      if (close == std::string_view::npos) {
        return false;
      }
      auto host = input.substr(1, close - 1);
      if (host.empty() or not parsers::ipv6(host)) {
        return false;
      }
      result.host = std::string{host};
      auto rest = input.substr(close + 1);
      if (rest.empty()) {
        e = std::move(result);
        f = l;
        return true;
      }
      if (not rest.starts_with(':')) {
        return false;
      }
      auto parsed_port = parse_port(rest.substr(1));
      if (not parsed_port) {
        return false;
      }
      result.port = *parsed_port;
      e = std::move(result);
      f = l;
      return true;
    }
    if (parsers::ipv6(input)) {
      result.host = std::string{input};
      e = std::move(result);
      f = l;
      return true;
    }
    if (auto colon = input.rfind(':'); colon != std::string_view::npos) {
      auto parsed_port = parse_port(input.substr(colon + 1));
      if (not parsed_port or input.substr(0, colon).contains(':')) {
        return false;
      }
      auto host = input.substr(0, colon);
      if (not is_hostname(host)) {
        return false;
      }
      result.host = std::string{host};
      result.port = *parsed_port;
      e = std::move(result);
      f = l;
      return true;
    }
    if (not is_hostname(input)) {
      return false;
    }
    result.host = std::string{input};
    e = std::move(result);
    f = l;
    return true;
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
