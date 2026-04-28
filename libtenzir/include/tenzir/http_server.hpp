//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

// This header exposes the proxygen-dependent HTTP server utilities.
// It is intentionally NOT included by http.hpp to avoid pulling in proxygen
// headers everywhere. Only include this from operators that actually run an
// HTTP server (accept_http, accept_opensearch) and from http.cpp.

#include "tenzir/diagnostics.hpp"
#include "tenzir/http.hpp"
#include "tenzir/option.hpp"
#include "tenzir/tls_options.hpp"

#include <proxygen/lib/http/coro/HTTPSourceHolder.h>
#include <wangle/ssl/SSLContextConfig.h>

#include <charconv>
#include <cstdint>
#include <string>
#include <string_view>

namespace tenzir::http {

auto make_folly_tls_config(Option<located<data>> const& tls, location primary,
                           diagnostic_handler& dh, tls_options::options options)
  -> failure_or<wangle::SSLContextConfig>;

} // namespace tenzir::http

namespace tenzir::http_server {

template <class T>
auto parse_number(std::string_view text) -> Option<T> {
  if (text.empty()) {
    return None{};
  }
  auto value = T{};
  auto const* begin = text.data();
  auto const* end = begin + text.size();
  auto [ptr, ec] = std::from_chars(begin, end, value);
  if (ec != std::errc{} or ptr != end) {
    return None{};
  }
  return value;
}

struct server_endpoint {
  std::string host;
  uint16_t port;
  Option<bool> scheme_tls;
};

auto parse_endpoint(std::string_view endpoint, location loc,
                    diagnostic_handler& dh,
                    std::string_view argument_name = "endpoint")
  -> Option<server_endpoint>;

auto is_tls_enabled(Option<located<data>> const& tls) -> bool;

auto make_response(uint16_t status, const std::string& content_type,
                   std::string body) -> proxygen::coro::HTTPSourceHolder;

} // namespace tenzir::http_server
