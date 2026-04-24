//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/http.hpp"

#include "tenzir/curl.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/http_server.hpp"

#include <proxygen/lib/http/coro/HTTPFixedSource.h>
#include <proxygen/lib/http/coro/server/HTTPServer.h>
#include <proxygen/lib/utils/URL.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <string_view>

namespace tenzir::http {

namespace {} // namespace

auto add_default_url_scheme(std::string& url, bool tls_enabled) -> void {
  if (not url.starts_with("http://") and not url.starts_with("https://")) {
    url.insert(0, tls_enabled ? "https://" : "http://");
  }
}

auto is_tls_enabled(Option<located<data>> const& tls,
                    tls_options::options options,
                    std::string_view url_when_missing) -> bool {
  if (tls) {
    auto tls_opts = tls_options{*tls, options};
    return tls_opts.get_tls(nullptr).inner;
  }
  return not url_when_missing.starts_with("http://");
}

auto normalize_url_and_tls(Option<located<data>> const& tls, std::string& url,
                           location url_loc, diagnostic_handler& dh,
                           tls_options::options options) -> failure_or<bool> {
  auto tls_opts = tls ? tls_options{*tls, options} : tls_options{options};
  TRY(tls_opts.validate(url, url_loc, dh));
  add_default_url_scheme(url, tls_opts.get_tls(nullptr).inner);
  return url.starts_with("https://");
}

auto make_header_secret_requests(
  Option<located<data>> const& headers,
  std::vector<std::pair<std::string, std::string>>& resolved_headers,
  diagnostic_handler& dh) -> std::vector<secret_request> {
  resolved_headers.clear();
  auto result = std::vector<secret_request>{};
  if (not headers) {
    return result;
  }
  auto const* rec = try_as<record>(headers->inner);
  TENZIR_ASSERT(rec);
  resolved_headers.reserve(rec->size());
  for (auto const& [header_name, value] : *rec) {
    match(
      value,
      [&](std::string const& literal) {
        resolved_headers.emplace_back(header_name, literal);
      },
      [&](secret const& sec) {
        auto& out
          = resolved_headers.emplace_back(header_name, std::string{}).second;
        result.emplace_back(
          make_secret_request(header_name, sec, headers->source, out, dh));
      },
      [](auto const&) {
        TENZIR_UNREACHABLE();
      });
  }
  return result;
}

auto parse_folly_tls_version(std::string_view input)
  -> Option<folly::SSLContext::SSLVersion> {
  if (input == "" or input == "any" or input == "1.0") {
    return folly::SSLContext::SSLVersion::TLSv1;
  }
  if (input == "1.2") {
    return folly::SSLContext::SSLVersion::TLSv1_2;
  }
  if (input == "1.3") {
    return folly::SSLContext::SSLVersion::TLSv1_3;
  }
  return None{};
}

auto make_folly_tls_config(Option<located<data>> const& tls, location primary,
                           diagnostic_handler& dh, tls_options::options options)
  -> failure_or<wangle::SSLContextConfig> {
  auto tls_opts = tls ? tls_options{*tls, options} : tls_options{options};
  auto certfile = tls_opts.get_certfile(nullptr);
  if (not certfile) {
    diagnostic::error("`tls.certfile` is required when TLS is enabled")
      .primary(primary)
      .emit(dh);
    return failure::promise();
  }
  auto keyfile = tls_opts.get_keyfile(nullptr);
  auto password = tls_opts.get_password(nullptr);
  auto config = proxygen::coro::HTTPServer::getDefaultTLSConfig();
  if (auto min = tls_opts.get_tls_min_version(nullptr)) {
    if (not min->inner.empty()) {
      if (auto parsed = parse_folly_tls_version(min->inner)) {
        config.sslVersion = *parsed;
      } else {
        diagnostic::error("invalid TLS minimum version: `{}`", min->inner)
          .primary(*min)
          .hint("supported values are `1.0`, `1.2`, and `1.3`")
          .emit(dh);
        return failure::promise();
      }
    }
  }
  try {
    config.setCertificate(certfile->inner,
                          keyfile ? keyfile->inner : certfile->inner,
                          password ? password->inner : "");
  } catch (std::exception const& ex) {
    diagnostic::error("failed to load TLS certificate: {}", ex.what())
      .primary(*certfile)
      .emit(dh);
    return failure::promise();
  }
  auto require_client_cert
    = tls_opts.get_tls_require_client_cert(nullptr).inner;
  auto skip_peer_verification
    = tls_opts.get_skip_peer_verification(nullptr).inner;
  if (require_client_cert) {
    config.clientVerification
      = folly::SSLContext::VerifyClientCertificate::ALWAYS;
  } else if (skip_peer_verification) {
    config.clientVerification
      = folly::SSLContext::VerifyClientCertificate::DO_NOT_REQUEST;
  } else {
    config.clientVerification
      = folly::SSLContext::VerifyClientCertificate::IF_PRESENTED;
  }
  if (auto client_ca = tls_opts.get_tls_client_ca(nullptr)) {
    config.clientCAFiles.push_back(client_ca->inner);
  }
  if (auto cacert = tls_opts.get_cacert(nullptr)) {
    config.clientCAFiles.push_back(cacert->inner);
  }
  return config;
}

auto message::header(const std::string& name) -> struct header* {
  auto pred = [&](auto& x) -> bool {
    if (x.name.size() != name.size()) {
      return false;
    }
    for (auto i = 0u; i < name.size(); ++i) {
      if (::toupper(x.name[i]) != ::toupper(name[i])) {
        return false;
      }
    }
    return true;
  };
  auto i = std::find_if(headers.begin(), headers.end(), pred);
  return i == headers.end() ? nullptr : &*i;
}

auto message::header(const std::string& name) const -> const struct header* {
  // We use a const_cast to avoid duplicating logic.
  auto* self = const_cast<message*>(this);
  return self->header(name);
}

auto request_item::parse(std::string_view str) -> std::optional<request_item> {
  auto is_valid_header_name = [](std::string_view name) {
    for (char c : name) {
      if (std::isalnum(static_cast<unsigned char>(c)) == 0 and c != '-'
          and c != '_') {
        return false;
      }
    }
    return true;
  };
  auto xs = detail::split_escaped(str, ":=@", "\\", 1);
  if (xs.size() == 2) {
    return request_item{.type = file_data_json, .key = xs[0], .value = xs[1]};
  }
  xs = detail::split_escaped(str, ":=", "\\", 1);
  if (xs.size() == 2) {
    return request_item{.type = data_json, .key = xs[0], .value = xs[1]};
  }
  xs = detail::split_escaped(str, ":", "\\", 1);
  if (xs.size() == 2 and is_valid_header_name(xs[0])) {
    return request_item{.type = header, .key = xs[0], .value = xs[1]};
  }
  xs = detail::split_escaped(str, "==", "\\", 1);
  if (xs.size() == 2) {
    return request_item{.type = url_param, .key = xs[0], .value = xs[1]};
  }
  xs = detail::split_escaped(str, "=@", "\\", 1);
  if (xs.size() == 2) {
    return request_item{.type = file_data, .key = xs[0], .value = xs[1]};
  }
  xs = detail::split_escaped(str, "@", "\\", 1);
  if (xs.size() == 2) {
    return request_item{.type = file_form, .key = xs[0], .value = xs[1]};
  }
  xs = detail::split_escaped(str, "=", "\\", 1);
  if (xs.size() == 2) {
    return request_item{.type = data, .key = xs[0], .value = xs[1]};
  }
  return {};
}

auto apply(std::vector<request_item> items, request& req) -> caf::error {
  auto body = record{};
  for (auto& item : items) {
    switch (item.type) {
      case request_item::header: {
        req.headers.emplace_back(std::move(item.key), std::move(item.value));
        break;
      }
      case request_item::data: {
        if (req.method.empty()) {
          req.method = "POST";
        }
        body.emplace(std::move(item.key), std::move(item.value));
        break;
      }
      case request_item::data_json: {
        if (req.method.empty()) {
          req.method = "POST";
        }
        auto data = from_json(item.value);
        if (not data) {
          return data.error();
        }
        body.emplace(std::move(item.key), std::move(*data));
        break;
      }
      case request_item::url_param: {
        auto pos = req.uri.find('?');
        if (pos == std::string::npos) {
          req.uri += '?';
        } else if (pos + 1 != req.uri.size()) {
          req.uri += '&';
        }
        req.uri += fmt::format("{}={}", curl::escape(item.key),
                               curl::escape(item.value));
        break;
      }
      default:
        return caf::make_error(ec::unimplemented, "unsupported item type");
    }
  }
  auto json_encode = [](const auto& x) {
    auto result = to_json(x, {.oneline = true});
    TENZIR_ASSERT(result);
    return std::move(*result);
  };
  auto url_encode = [](const auto& x) {
    return curl::escape(flatten(x));
  };

  // We assemble an Accept header as we go, unless we have one already.
  auto accept = std::optional<std::vector<std::string>>{};
  if (req.header("Accept") == nullptr) {
    accept = {"*/*"};
  }
  // If the user provided any request body data, we default to JSON encoding.
  // The user can override this behavior by setting a Content-Type header.
  const auto* content_type_header = req.header("Content-Type");
  if (content_type_header != nullptr
      and not content_type_header->value.empty()) {
    // Encode request body based on provided Content-Type header value.
    const auto& content_type = content_type_header->value;
    if (content_type.starts_with("application/x-www-form-urlencoded")) {
      if (not body.empty()) {
        req.body = url_encode(body);
        TENZIR_DEBUG("urlencoded request body: {}", req.body);
      }
    } else if (content_type.starts_with("application/json")) {
      if (not body.empty()) {
        req.body = json_encode(body);
        if (accept) {
          accept->insert(accept->begin(), "application/json");
        }
        TENZIR_DEBUG("JSON-encoded request body: {}", req.body);
      }
    } else {
      return caf::make_error(ec::parse_error,
                             fmt::format("cannot encode HTTP request body "
                                         "with Content-Type: {}",
                                         content_type));
    }
  } else if (not body.empty()) {
    // Without a Content-Type, we assume JSON.
    req.body = json_encode(body);
    req.headers.emplace_back("Content-Type", "application/json");
    if (accept) {
      accept->insert(accept->begin(), "application/json");
    }
  }
  // Add an Accept header unless we have one already.
  if (accept) {
    auto value = fmt::format("{}", fmt::join(*accept, ", "));
    req.headers.emplace_back("Accept", std::move(value));
  }
  return {};
}

auto make_decompressor(std::string_view encoding, diagnostic_handler& dh)
  -> Option<std::shared_ptr<arrow::util::Decompressor>> {
  if (encoding.empty()) {
    return None{};
  }
  auto compression_type
    = arrow::util::Codec::GetCompressionType(std::string{encoding});
  if (not compression_type.ok()) {
    diagnostic::warning("invalid Content-Encoding: `{}`", encoding)
      .hint("must be one of `brotli`, `bz2`, `gzip`, `lz4`, `zstd`")
      .note("skipping decompression")
      .emit(dh);
    return None{};
  }
  auto codec = arrow::util::Codec::Create(
    compression_type.ValueUnsafe(), arrow::util::kUseDefaultCompressionLevel);
  if (not codec.ok() or not codec.ValueUnsafe()) {
    diagnostic::warning("failed to create codec for Content-Encoding: `{}`",
                        encoding)
      .note("skipping decompression")
      .emit(dh);
    return None{};
  }
  auto dec = codec.ValueUnsafe()->MakeDecompressor();
  if (not dec.ok()) {
    diagnostic::warning("failed to create decompressor for Content-Encoding: "
                        "`{}`",
                        encoding)
      .note("skipping decompression")
      .emit(dh);
    return None{};
  }
  return std::move(dec.ValueUnsafe());
}

auto decompress_chunk(arrow::util::Decompressor& decompressor,
                      std::span<std::byte const> input, diagnostic_handler& dh,
                      size_t max_output_size) -> Result<blob, uint16_t> {
  auto out = blob{};
  auto initial_size
    = std::min(max_output_size, std::max<size_t>(input.size_bytes() * 2, 64));
  out.resize(initial_size);
  auto written = size_t{};
  auto read = size_t{};
  while (read < input.size_bytes()) {
    auto result = decompressor.Decompress(
      detail::narrow<int64_t>(input.size_bytes() - read),
      reinterpret_cast<uint8_t const*>(input.data() + read),
      detail::narrow<int64_t>(out.size() - written),
      reinterpret_cast<uint8_t*>(out.data() + written));
    if (not result.ok()) {
      diagnostic::warning("failed to decompress: {}",
                          result.status().ToString())
        .note("emitting compressed body")
        .emit(dh);
      return Err{(uint16_t)400}; // bad request
    }
    auto const bytes_written = detail::narrow<size_t>(result->bytes_written);
    if (bytes_written > max_output_size - written) [[unlikely]] {
      diagnostic::warning("decompressed output exceeds limit").emit(dh);
      return Err{(uint16_t)413}; // payload too large
    }
    written += bytes_written;
    read += detail::narrow<size_t>(result->bytes_read);
    if (result->need_more_output) {
      if (out.size() >= max_output_size) [[unlikely]] {
        diagnostic::warning("decompressed output exceeds limit").emit(dh);
        return Err{(uint16_t)413}; // payload too large
      }
      auto next_size = std::min(out.size() * 2, max_output_size);
      out.resize(next_size);
    }
    // Reset gracefully when a compressed stream ends to handle concatenated
    // compressed streams (e.g. multiple gzip members in one body).
    if (decompressor.IsFinished()) {
      if (auto reset = decompressor.Reset(); not reset.ok()) {
        diagnostic::warning("failed to reset decompressor: {}",
                            reset.ToString())
          .note("emitting compressed body")
          .emit(dh);
        return Err{(uint16_t)400}; // bad request
      }
    }
  }
  out.resize(written);
  return out;
}

} // namespace tenzir::http

namespace tenzir::http_server {

auto parse_endpoint(std::string_view endpoint, location loc,
                    diagnostic_handler& dh, std::string_view argument_name)
  -> Option<server_endpoint> {
  if (endpoint.contains("://")) {
    auto parsed = proxygen::URL{std::string{endpoint}};
    if (not parsed.isValid() or not parsed.hasHost()) {
      diagnostic::error("failed to parse endpoint URL").primary(loc).emit(dh);
      return None{};
    }
    auto scheme = parsed.getScheme();
    auto scheme_tls = Option<bool>{None{}};
    if (scheme == "https") {
      scheme_tls = true;
    } else if (scheme == "http") {
      scheme_tls = false;
    } else {
      diagnostic::error("unsupported endpoint URL scheme: `{}`", scheme)
        .primary(loc)
        .hint("use `http://` or `https://`")
        .emit(dh);
      return None{};
    }
    return server_endpoint{
      .host = parsed.getHost(),
      .port = parsed.getPort(),
      .scheme_tls = scheme_tls,
    };
  }
  if (endpoint.empty()) {
    diagnostic::error("`{}` must not be empty", argument_name)
      .primary(loc)
      .emit(dh);
    return None{};
  }
  if (endpoint.front() == '[') {
    auto const close = endpoint.find(']');
    if (close == std::string_view::npos) {
      diagnostic::error("invalid IPv6 endpoint syntax")
        .primary(loc)
        .hint("expected `[host]:port`")
        .emit(dh);
      return None{};
    }
    auto const host = endpoint.substr(1, close - 1);
    auto const rest = endpoint.substr(close + 1);
    if (rest.empty() or rest.front() != ':') {
      diagnostic::error("invalid IPv6 endpoint syntax")
        .primary(loc)
        .hint("expected `[host]:port`")
        .emit(dh);
      return None{};
    }
    auto port = parse_number<uint16_t>(rest.substr(1));
    if (not port) {
      diagnostic::error("failed to parse endpoint port").primary(loc).emit(dh);
      return None{};
    }
    return server_endpoint{
      .host = std::string{host},
      .port = *port,
      .scheme_tls = None{},
    };
  }
  auto const colon = endpoint.rfind(':');
  if (colon == std::string_view::npos) {
    diagnostic::error("failed to parse endpoint")
      .primary(loc)
      .hint("expected `host:port`, `[host]:port`, or URL")
      .emit(dh);
    return None{};
  }
  auto const host = endpoint.substr(0, colon);
  auto port = parse_number<uint16_t>(endpoint.substr(colon + 1));
  if (not port) {
    diagnostic::error("failed to parse endpoint port").primary(loc).emit(dh);
    return None{};
  }
  return server_endpoint{
    .host = std::string{host},
    .port = *port,
    .scheme_tls = None{},
  };
}

auto is_tls_enabled(Option<located<data>> const& tls) -> bool {
  if (not tls) {
    return false;
  }
  auto tls_opts = tls_options{*tls, {.tls_default = false, .is_server = true}};
  return tls_opts.get_tls(nullptr).inner;
}

auto make_response(uint16_t status, const std::string& content_type,
                   std::string body) -> proxygen::coro::HTTPSourceHolder {
  auto* source = proxygen::coro::HTTPFixedSource::makeFixedResponse(
    status, std::move(body));
  if (not content_type.empty()) {
    source->msg_->getHeaders().set(proxygen::HTTP_HEADER_CONTENT_TYPE,
                                   content_type);
  }
  return proxygen::coro::HTTPSourceHolder{source};
}

} // namespace tenzir::http_server
