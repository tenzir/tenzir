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
#include "tenzir/series_builder.hpp"
#include "tenzir/view3.hpp"

#include <boost/url/parse.hpp>
#include <boost/url/url.hpp>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <string_view>

namespace tenzir::http {

namespace {

auto normalize_content_encoding(std::string_view encoding) -> std::string {
  auto result = std::string{detail::trim(encoding)};
  std::transform(result.begin(), result.end(), result.begin(), [](char c) {
    return static_cast<char>(
      detail::ascii_tolower(static_cast<unsigned char>(c)));
  });
  return result;
}

// Splits a raw HTTP Link header value into individual link-value items at
// top-level commas (not inside <...> or quoted strings).
auto split_link_header(std::string_view value)
  -> std::vector<std::string_view> {
  auto result = std::vector<std::string_view>{};
  auto start = size_t{0};
  auto in_angle = false;
  auto in_quotes = false;
  auto escaped = false;
  for (auto i = size_t{}; i < value.size(); ++i) {
    const auto c = value[i];
    if (escaped) {
      escaped = false;
      continue;
    }
    if (c == '\\' and in_quotes) {
      escaped = true;
      continue;
    }
    if (c == '"' and not in_angle) {
      in_quotes = not in_quotes;
      continue;
    }
    if (c == '<' and not in_quotes) {
      in_angle = true;
      continue;
    }
    if (c == '>' and not in_quotes) {
      in_angle = false;
      continue;
    }
    if (c == ',' and not in_quotes and not in_angle) {
      auto item = detail::trim(value.substr(start, i - start));
      if (not item.empty()) {
        result.push_back(item);
      }
      start = i + 1;
    }
  }
  auto item = detail::trim(value.substr(start));
  if (not item.empty()) {
    result.push_back(item);
  }
  return result;
}

// Splits link-value parameters at semicolons, honouring quoted-string escaping.
auto split_link_params(std::string_view value)
  -> std::pair<std::vector<std::string_view>, bool> {
  auto result = std::vector<std::string_view>{};
  auto start = size_t{0};
  auto in_quotes = false;
  auto escaped = false;
  for (auto i = size_t{}; i < value.size(); ++i) {
    const auto c = value[i];
    if (escaped) {
      escaped = false;
      continue;
    }
    if (c == '\\' and in_quotes) {
      escaped = true;
      continue;
    }
    if (c == '"') {
      in_quotes = not in_quotes;
      continue;
    }
    if (c == ';' and not in_quotes) {
      auto item = detail::trim(value.substr(start, i - start));
      if (not item.empty()) {
        result.push_back(item);
      }
      start = i + 1;
    }
  }
  auto item = detail::trim(value.substr(start));
  if (not item.empty()) {
    result.push_back(item);
  }
  return {std::move(result), not in_quotes};
}

// Returns true when "next" appears as a token in a rel parameter value.
auto rel_contains_next(std::string_view value) -> bool {
  value = detail::trim(value);
  if (value.size() >= 2 and value.front() == '"' and value.back() == '"') {
    value.remove_prefix(1);
    value.remove_suffix(1);
  }
  auto index = size_t{};
  while (index < value.size()) {
    while (index < value.size()
           and std::isspace(static_cast<unsigned char>(value[index])) != 0) {
      ++index;
    }
    const auto token_begin = index;
    while (index < value.size()
           and std::isspace(static_cast<unsigned char>(value[index])) == 0) {
      ++index;
    }
    const auto token = value.substr(token_begin, index - token_begin);
    if (not token.empty() and detail::ascii_icase_equal(token, "next")) {
      return true;
    }
  }
  return false;
}

// Parses a single RFC 8288 link-value and extracts its `rel=next` target URI.
// Returns Err(Unit) for malformed link-values.
auto next_link_target(std::string_view header)
  -> Result<Option<std::string_view>, Unit> {
  auto item = detail::trim(header);
  if (item.empty()) {
    return None{};
  }
  if (item.front() != '<') {
    return Err{Unit{}};
  }
  const auto uri_end = item.find('>');
  if (uri_end == std::string_view::npos) {
    return Err{Unit{}};
  }
  auto target = item.substr(1, uri_end - 1);
  auto params = item.substr(uri_end + 1);
  auto [param_parts, ok] = split_link_params(params);
  if (not ok) {
    return Err{Unit{}};
  }
  auto has_next = false;
  for (const auto part : param_parts) {
    const auto eq = part.find('=');
    auto name = detail::trim(part.substr(0, eq));
    if (name.empty() or not detail::ascii_icase_equal(name, "rel")) {
      continue;
    }
    if (eq == std::string_view::npos) {
      continue;
    }
    if (rel_contains_next(detail::trim(part.substr(eq + 1)))) {
      has_next = true;
      break;
    }
  }
  if (has_next) {
    return Option<std::string_view>{target};
  }
  return None{};
}

auto emit_odata_envelope_error(location paginate_loc, diagnostic_handler& dh)
  -> failure_or<OdataPage> {
  diagnostic::error(
    "expected OData response body to contain a top-level `value` array")
    .primary(paginate_loc)
    .emit(dh);
  return failure::promise();
}

} // namespace

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

auto make_http_pool_config(Option<located<data>> const& tls, std::string& url,
                           location url_loc, diagnostic_handler& dh,
                           std::chrono::milliseconds request_timeout,
                           tls_options::options options)
  -> failure_or<HttpPoolConfig> {
  TRY(auto tls_enabled, normalize_url_and_tls(tls, url, url_loc, dh, options));
  auto config = HttpPoolConfig{
    .tls = tls_enabled,
    .ssl_context = nullptr,
    .request_timeout = request_timeout,
  };
  if (tls_enabled) {
    auto tls_opts = tls ? tls_options{*tls, options} : tls_options{options};
    TRY(auto ssl_context, tls_opts.make_folly_ssl_context(dh));
    config.ssl_context = std::move(ssl_context);
  }
  return config;
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

auto compress_request_body(std::string body, std::string_view encoding,
                           diagnostic_handler& dh, location loc)
  -> encoded_request_body {
  auto normalized_encoding = normalize_content_encoding(encoding);
  if (normalized_encoding.empty()) {
    return {.body = std::move(body)};
  }
  auto compression_type
    = arrow::util::Codec::GetCompressionType(normalized_encoding);
  if (not compression_type.ok()) {
    diagnostic::warning("invalid Content-Encoding: `{}`", encoding)
      .primary(loc)
      .hint("must be one of `brotli`, `bz2`, `gzip`, `lz4`, `zstd`")
      .note("sending uncompressed body")
      .emit(dh);
    return {.body = std::move(body)};
  }
  auto codec = arrow::util::Codec::Create(
    compression_type.ValueUnsafe(), arrow::util::kUseDefaultCompressionLevel);
  if (not codec.ok() or not codec.ValueUnsafe()) {
    diagnostic::warning("failed to create codec for Content-Encoding: `{}`",
                        normalized_encoding)
      .primary(loc)
      .note("sending uncompressed body")
      .emit(dh);
    return {.body = std::move(body)};
  }
  auto const* input = reinterpret_cast<uint8_t const*>(body.data());
  auto const input_size = detail::narrow<int64_t>(body.size());
  auto compressed = std::string{};
  compressed.resize(codec.ValueUnsafe()->MaxCompressedLen(input_size, input));
  auto* output = reinterpret_cast<uint8_t*>(compressed.data());
  auto result = codec.ValueUnsafe()->Compress(
    input_size, input, detail::narrow<int64_t>(compressed.size()), output);
  if (not result.ok()) {
    diagnostic::warning("failed to compress request body: {}",
                        result.status().ToStringWithoutContextLines())
      .primary(loc)
      .note("sending uncompressed body")
      .emit(dh);
    return {.body = std::move(body)};
  }
  compressed.resize(detail::narrow<size_t>(result.ValueUnsafe()));
  return {
    .body = std::move(compressed),
    .content_encoding = std::move(normalized_encoding),
  };
}

auto add_request_body_headers(std::map<std::string, std::string>& headers,
                              encoded_request_body const& body) -> void {
  headers["Content-Length"] = fmt::to_string(body.body.size());
  if (body.content_encoding) {
    headers["Content-Encoding"] = *body.content_encoding;
  }
}

auto make_decompressor(std::string_view encoding, diagnostic_handler& dh)
  -> Option<std::shared_ptr<arrow::util::Decompressor>> {
  auto normalized_encoding = normalize_content_encoding(encoding);
  if (normalized_encoding.empty()) {
    return None{};
  }
  auto compression_type
    = arrow::util::Codec::GetCompressionType(normalized_encoding);
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
                        normalized_encoding)
      .note("skipping decompression")
      .emit(dh);
    return None{};
  }
  auto dec = codec.ValueUnsafe()->MakeDecompressor();
  if (not dec.ok()) {
    diagnostic::warning("failed to create decompressor for Content-Encoding: "
                        "`{}`",
                        normalized_encoding)
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

auto parse_pagination_mode(std::string_view mode) -> Option<PaginationMode> {
  if (mode == "link") {
    return PaginationMode::link;
  }
  if (mode == "odata") {
    return PaginationMode::odata;
  }
  return None{};
}

auto next_url_from_link_headers(
  std::vector<std::pair<std::string, std::string>> const& response_headers,
  std::string const& base_url, location paginate_loc, diagnostic_handler& dh)
  -> Option<std::string> {
  auto base = boost::urls::parse_uri_reference(base_url);
  if (not base) {
    diagnostic::warning("failed to parse request URI for link pagination: {}",
                        base.error().message())
      .primary(paginate_loc)
      .note("stopping pagination")
      .emit(dh);
    return None{};
  }
  auto malformed = false;
  for (auto const& [name, value] : response_headers) {
    if (not detail::ascii_icase_equal(name, "link")) {
      continue;
    }
    for (auto header : split_link_header(value)) {
      auto parsed = next_link_target(header);
      if (parsed.is_err()) {
        malformed = true;
        continue;
      }
      auto target = parsed.unwrap();
      if (not target) {
        continue;
      }
      auto ref = boost::urls::parse_uri_reference(*target);
      if (not ref) {
        diagnostic::warning("invalid `rel=next` URL in Link header: {}",
                            ref.error().message())
          .primary(paginate_loc)
          .note("stopping pagination")
          .emit(dh);
        return None{};
      }
      auto resolved = boost::urls::url{};
      if (auto r = boost::urls::resolve(*base, *ref, resolved); not r) {
        diagnostic::warning("failed to resolve `rel=next` URL: {}",
                            r.error().message())
          .primary(paginate_loc)
          .note("stopping pagination")
          .emit(dh);
        return None{};
      }
      return Option<std::string>{std::string{resolved.buffer()}};
    }
  }
  if (malformed) {
    diagnostic::warning("failed to parse Link header for pagination")
      .primary(paginate_loc)
      .note("stopping pagination")
      .emit(dh);
  }
  return None{};
}

auto extract_odata_page(table_slice const& slice, location paginate_loc,
                        diagnostic_handler& dh) -> failure_or<OdataPage> {
  if (slice.rows() != 1) {
    return emit_odata_envelope_error(paginate_loc, dh);
  }
  auto result = OdataPage{};
  auto builder = series_builder{};
  auto saw_body = false;
  for (auto body : values3(slice)) {
    saw_body = true;
    auto value = Option<data_view3>{None{}};
    for (auto const& [key, field] : body) {
      if (key == "@odata.nextLink") {
        if (auto const* next = try_as<std::string_view>(&field)) {
          result.next_url = std::string{*next};
        }
        continue;
      }
      if (key == "value") {
        value = field;
      }
    }
    if (not value) {
      return emit_odata_envelope_error(paginate_loc, dh);
    }
    auto const* items = try_as<view3<list>>(&*value);
    if (not items) {
      return emit_odata_envelope_error(paginate_loc, dh);
    }
    for (auto item : *items) {
      auto const* item_record = try_as<view3<record>>(item);
      if (not item_record) {
        diagnostic::error("expected OData `value` array to contain objects")
          .primary(paginate_loc)
          .emit(dh);
        return failure::promise();
      }
      auto output = builder.record();
      for (auto const& [key, field] : *item_record) {
        if (key.starts_with("@odata.")) {
          continue;
        }
        output.field(key, field);
      }
    }
  }
  if (not saw_body) {
    return emit_odata_envelope_error(paginate_loc, dh);
  }
  result.events = builder.finish_as_table_slice();
  return result;
}

} // namespace tenzir::http
