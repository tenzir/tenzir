//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/web_identity.hpp"

#include "tenzir/chunk.hpp"
#include "tenzir/curl.hpp"
#include "tenzir/detail/load_contents.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/http.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/transfer.hpp"
#include "tenzir/try.hpp"

#include <proxygen/lib/http/codec/CodecUtil.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <filesystem>
#include <simdjson.h>

namespace tenzir {

namespace {

auto as_bytes(std::string_view s) -> folly::ByteRange {
  return {reinterpret_cast<const unsigned char*>(s.data()), s.size()};
}

/// Rejects everything HTTP forbids in a header, most importantly the CR and LF
/// that would let a value smuggle additional headers into the request. We use
/// the lenient mode because the strict one also rejects uppercase, which is
/// only a requirement of HTTP/2.
auto is_valid_header_name(std::string_view s) -> bool {
  return proxygen::CodecUtil::validateHeaderName(
    as_bytes(s), proxygen::CodecUtil::HEADER_NAME_STRICT_COMPAT);
}

auto is_valid_header_value(std::string_view s) -> bool {
  return proxygen::CodecUtil::validateHeaderValue(as_bytes(s),
                                                  proxygen::CodecUtil::STRICT);
}

/// Parses the record of `string` or `secret` values at `key`, such as
/// `headers` or `query_params`. @param what names one entry in diagnostics.
/// @param is_header enables HTTP header validation; query parameters need none
/// because `make_uri()` percent-encodes them.
auto parse_secret_record(const located<record>& config, std::string_view key,
                         std::string_view what, bool is_header,
                         Option<std::vector<std::pair<std::string, secret>>>& x,
                         diagnostic_handler& dh) -> failure_or<void> {
  auto it = config.inner.find(key);
  if (it == config.inner.end()) {
    return {};
  }
  auto* r = try_as<record>(it->second.get_data());
  if (not r) {
    diagnostic::error("`{}` must be a record", key).primary(config).emit(dh);
    return failure::promise();
  }
  x = std::vector<std::pair<std::string, secret>>{};
  for (auto& [name, value] : *r) {
    if (is_header and not is_valid_header_name(name)) {
      diagnostic::error("'{}' is not a valid HTTP header name", name)
        .primary(config)
        .emit(dh);
      return failure::promise();
    }
    if (auto* s = try_as<secret>(value.get_data())) {
      x->emplace_back(name, std::move(*s));
    } else if (auto* str = try_as<std::string>(value.get_data())) {
      if (str->empty()) {
        diagnostic::error("{} '{}' value must not be empty", what, name)
          .primary(config)
          .emit(dh);
        return failure::promise();
      }
      if (is_header and not is_valid_header_value(*str)) {
        diagnostic::error("header '{}' has an invalid value", name)
          .primary(config)
          .emit(dh);
        return failure::promise();
      }
      x->emplace_back(name, secret::make_literal(std::move(*str)));
    } else {
      diagnostic::error("{} '{}' value must be a `string` or `secret`", what,
                        name)
        .primary(config)
        .emit(dh);
      return failure::promise();
    }
  }
  return {};
}

} // namespace

auto assign_secret(const located<record>& config, std::string_view key,
                   Option<secret>& x, diagnostic_handler& dh)
  -> failure_or<void> {
  if (auto it = config.inner.find(key); it != config.inner.end()) {
    if (auto* s = try_as<secret>(it->second.get_data())) {
      x = std::move(*s);
    } else if (auto* str = try_as<std::string>(it->second.get_data())) {
      if (str->empty()) {
        diagnostic::error("'{}' must not be empty", key)
          .primary(config)
          .emit(dh);
        return failure::promise();
      }
      x = secret::make_literal(std::move(*str));
    } else {
      diagnostic::error("'{}' must be a `string` or `secret`", key)
        .primary(config)
        .emit(dh);
      return failure::promise();
    }
  }
  return {};
}

auto token_endpoint_options::from_record(located<record> config,
                                         diagnostic_handler& dh)
  -> failure_or<token_endpoint_options> {
  constexpr auto known = std::array{"url", "headers", "query_params", "path"};
  const auto unknown = std::ranges::find_if(config.inner, [&](auto&& x) {
    return std::ranges::find(known, x.first) == std::ranges::end(known);
  });
  if (unknown != std::ranges::end(config.inner)) {
    diagnostic::error("unknown key '{}' in token_endpoint config",
                      (*unknown).first)
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  auto opts = token_endpoint_options{};
  opts.loc = config.source;
  TRY(assign_secret(config, "url", opts.url, dh));
  TRY(parse_secret_record(config, "headers", "header", true, opts.headers, dh));
  TRY(parse_secret_record(config, "query_params", "query parameter", false,
                          opts.query_params, dh));
  if (auto it = config.inner.find("path"); it != config.inner.end()) {
    if (is<caf::none_t>(it->second.get_data())) {
      opts.path_is_null = true;
    } else if (auto* str = try_as<std::string>(it->second.get_data())) {
      if (str->empty()) {
        diagnostic::error("`path` must not be empty").primary(config).emit(dh);
        return failure::promise();
      }
      opts.path = std::move(*str);
    } else {
      diagnostic::error("`path` must be a `string` or `null`")
        .primary(config)
        .emit(dh);
      return failure::promise();
    }
  }
  if (not opts.url) {
    diagnostic::error("`token_endpoint` requires `url`")
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  return opts;
}

auto web_identity_options::from_record(located<record> config,
                                       diagnostic_handler& dh)
  -> failure_or<web_identity_options> {
  constexpr auto known = std::array{"token_endpoint", "token_file", "token"};
  const auto unknown = std::ranges::find_if(config.inner, [&](auto&& x) {
    return std::ranges::find(known, x.first) == std::ranges::end(known);
  });
  if (unknown != std::ranges::end(config.inner)) {
    diagnostic::error("unknown key '{}' in web_identity config",
                      (*unknown).first)
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  auto opts = web_identity_options{};
  opts.loc = config.source;
  if (auto it = config.inner.find("token_endpoint"); it != config.inner.end()) {
    if (auto* r = try_as<record>(it->second.get_data())) {
      auto endpoint_config = located{std::move(*r), config.source};
      TRY(opts.token_endpoint,
          token_endpoint_options::from_record(std::move(endpoint_config), dh));
    } else {
      diagnostic::error("`token_endpoint` must be a record")
        .primary(config)
        .emit(dh);
      return failure::promise();
    }
  }
  TRY(assign_secret(config, "token_file", opts.token_file, dh));
  TRY(assign_secret(config, "token", opts.token, dh));
  const auto token_source_count = (opts.token_endpoint.has_value() ? 1 : 0)
                                  + (opts.token_file.has_value() ? 1 : 0)
                                  + (opts.token.has_value() ? 1 : 0);
  if (token_source_count == 0) {
    diagnostic::error(
      "`web_identity` requires one of: `token_endpoint`, `token_file`, `token`")
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  if (token_source_count > 1) {
    diagnostic::error(
      "`token_endpoint`, `token_file`, and `token` are mutually exclusive")
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  return opts;
}

auto web_identity_options::make_secret_requests(resolved_web_identity& resolved,
                                                diagnostic_handler& dh) const
  -> std::vector<secret_request> {
  auto requests = std::vector<secret_request>{};
  if (token_endpoint) {
    const auto& te = *token_endpoint;
    auto& rte = resolved.emplace<resolved_token_endpoint>();
    requests.emplace_back(
      make_secret_request("token_endpoint.url", *te.url, te.loc, rte.url, dh));
    if (te.headers) {
      // The requests reference `rte.headers` elements, so the vector must not
      // reallocate after the first request is made.
      rte.headers.reserve(te.headers->size());
      for (const auto& [key, value] : *te.headers) {
        rte.headers.emplace_back(key, std::string{});
        requests.emplace_back(
          make_secret_request(fmt::format("header '{}'", key), value, te.loc,
                              rte.headers.back().second, dh));
      }
    }
    if (te.query_params) {
      // As with the headers above, the requests point into the vector.
      rte.query_params.reserve(te.query_params->size());
      for (const auto& [key, value] : *te.query_params) {
        rte.query_params.emplace_back(key, std::string{});
        requests.emplace_back(
          make_secret_request(fmt::format("query parameter '{}'", key), value,
                              te.loc, rte.query_params.back().second, dh));
      }
    }
    if (te.path_is_null) {
      rte.path = None{};
    } else if (te.path) {
      rte.path = *te.path;
    } else {
      rte.path = ".access_token";
    }
  }
  if (token_file) {
    auto& file = resolved.emplace<resolved_token_file>();
    requests.emplace_back(
      make_secret_request("token_file", *token_file, loc, file.path, dh));
  }
  if (token) {
    auto& direct = resolved.emplace<resolved_token>();
    requests.emplace_back(
      make_secret_request("token", *token, loc, direct.token, dh));
  }
  return requests;
}

namespace {

auto fetch_token(const resolved_token& direct) -> caf::expected<std::string> {
  if (direct.token.empty()) {
    return diagnostic::error("web identity token is empty").to_error();
  }
  TENZIR_VERBOSE("using direct web identity token");
  return direct.token;
}

auto fetch_token(const resolved_token_file& file)
  -> caf::expected<std::string> {
  TENZIR_VERBOSE("reading web identity token from file: {}", file.path);
  const auto path = std::filesystem::path{file.path};
  constexpr auto max_token_file_size = std::uintmax_t{1024} * 1024;
  auto ec = std::error_code{};
  const auto file_size = std::filesystem::file_size(path, ec);
  if (ec) {
    return diagnostic::error("failed to check token file size")
      .note("file: {}", file.path)
      .note("{}", ec.message())
      .to_error();
  }
  if (file_size > max_token_file_size) {
    return diagnostic::error("token file is too large")
      .note("file: {}", file.path)
      .note("size: {} bytes, maximum: {} bytes", file_size, max_token_file_size)
      .to_error();
  }
  auto contents = detail::load_contents(file.path);
  if (not contents) {
    return diagnostic::error("failed to read web identity token file")
      .note("file: {}", file.path)
      .note("{}", contents.error())
      .to_error();
  }
  return detail::trim(*contents);
}

auto make_uri(const resolved_token_endpoint& te) -> std::string {
  auto uri = te.url;
  for (const auto& [key, value] : te.query_params) {
    // Token endpoint URLs often carry a query already, such as the
    // `api-version` in GitHub Actions' `ACTIONS_ID_TOKEN_REQUEST_URL`.
    uri += uri.contains('?') ? '&' : '?';
    fmt::format_to(std::back_inserter(uri), "{}={}", curl::escape(key),
                   curl::escape(value));
  }
  return uri;
}

auto fetch_token(const resolved_token_endpoint& te)
  -> caf::expected<std::string> {
  TENZIR_VERBOSE("fetching web identity token from endpoint");
  auto xfer = transfer{{}, TlsConfig::defaults()};
  auto req = http::Request{};
  req.uri = make_uri(te);
  req.method = "GET";
  for (const auto& [name, value] : te.headers) {
    req.headers.emplace_back(name, value);
  }
  if (auto err = xfer.prepare(req); err) {
    return diagnostic::error("failed to prepare web identity token request")
      .note("{}", err)
      .to_error();
  }
  xfer.handle().set(CURLOPT_TIMEOUT, 30L);
  constexpr auto max_response_size = size_t{1024} * 1024;
  auto body = std::string{};
  body.reserve(size_t{16} * 1024);
  for (auto&& chunk : xfer.download_chunks()) {
    if (not chunk) {
      return diagnostic::error("failed to fetch web identity token")
        .note("{}", chunk.error())
        .to_error();
    }
    if (*chunk) {
      if (body.size() + (*chunk)->size() > max_response_size) {
        return diagnostic::error("web identity token response too large")
          .note("maximum size: {} bytes", max_response_size)
          .to_error();
      }
      body.append(reinterpret_cast<const char*>((*chunk)->data()),
                  (*chunk)->size());
    }
  }
  auto [code, status] = xfer.handle().get<curl::easy::info::response_code>();
  if (code != curl::easy::code::ok) {
    return diagnostic::error("failed to get HTTP response status")
      .note("curl error: {}", to_string(code))
      .to_error();
  }
  if (status < 200 or status >= 300) {
    constexpr auto max_error_body_size = size_t{1024};
    auto error_body = body.size() > max_error_body_size
                        ? body.substr(0, max_error_body_size) + "..."
                        : body;
    return diagnostic::error("HTTP request failed")
      .note("status code: {}", status)
      .note("endpoint: {}", te.url)
      .note("response: {}", error_body)
      .to_error();
  }
  if (not te.path) {
    TENZIR_VERBOSE("treating web identity token response as plain text");
    return detail::trim(body);
  }
  TENZIR_VERBOSE("extracting web identity token from JSON path: {}", *te.path);
  // Only single-level paths like ".access_token" or ".token" are supported.
  auto path = *te.path;
  if (path.starts_with('.')) {
    path = path.substr(1);
  }
  // Validate path characters to prevent simdjson operator injection.
  const auto is_valid_path_char = [](char c) {
    return std::isalnum(static_cast<unsigned char>(c)) or c == '_' or c == '-';
  };
  if (not std::ranges::all_of(path, is_valid_path_char)) {
    return diagnostic::error("invalid JSON path for web identity token")
      .note("path: {}", *te.path)
      .note("only alphanumeric characters, underscores, and hyphens are "
            "allowed")
      .to_error();
  }
  if (path.empty()) {
    return diagnostic::error("invalid JSON path for web identity token")
      .note("path cannot be empty")
      .to_error();
  }
  auto parser = simdjson::ondemand::parser{};
  auto padded = simdjson::padded_string{body};
  auto doc = parser.iterate(padded);
  if (doc.error() != simdjson::SUCCESS) {
    return diagnostic::error("failed to parse web identity token response as "
                             "JSON")
      .note("error: {}", simdjson::error_message(doc.error()))
      .to_error();
  }
  auto token_value = doc[path];
  if (token_value.error() != simdjson::SUCCESS) {
    return diagnostic::error("failed to extract token from JSON response")
      .note("path: {}", *te.path)
      .note("error: {}", simdjson::error_message(token_value.error()))
      .to_error();
  }
  auto token_str = token_value.get_string();
  if (token_str.error() != simdjson::SUCCESS) {
    return diagnostic::error("web identity token is not a string")
      .note("path: {}", *te.path)
      .to_error();
  }
  return std::string{token_str.value()};
}

} // namespace

auto fetch_web_identity_token(const resolved_web_identity& web_identity)
  -> caf::expected<std::string> {
  return match(web_identity, [](const auto& source) {
    return fetch_token(source);
  });
}

} // namespace tenzir
