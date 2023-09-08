//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/config.hpp>
#include <tenzir/detail/string_literal.hpp>
#include <tenzir/location.hpp>
#include <tenzir/plugin.hpp>

#include <curl/curl.h>

#include <filesystem>
#include <regex>
#include <system_error>

using namespace std::chrono_literals;

namespace tenzir::plugins {

namespace {

/// HTTP options for curl.
struct http_options {
  http_options() {
    static const auto default_headers = std::map<std::string, std::string>{
      {"Accept", "*/*"},
      {"User-Agent", fmt::format("Tenzir/{}", version::version)},
    };
    headers = default_headers;
  }

  std::string method;
  std::map<std::string, std::string> headers;
  std::string body;
};

auto inspect(auto& f, http_options& x) -> bool {
  return f.object(x)
    .pretty_name("http_options")
    .fields(f.field("method", x.method), f.field("headers", x.headers),
            f.field("body", x.body));
}

/// Options for the curl wrapper.
struct curl_options {
  std::string default_protocol;
  std::string url;
  http_options http;
};

auto inspect(auto& f, curl_options& x) -> bool {
  return f.object(x)
    .pretty_name("curl_options")
    .fields(f.field("default_protocol", x.default_protocol),
            f.field("url", x.url), f.field("http", x.http));
}

/// A wrapper around the C API.
class curl {
  static auto to_error(CURLcode number) -> caf::error {
    if (number == CURLE_OK)
      return {};
    const auto* str = curl_easy_strerror(number);
    return caf::make_error(ec::unspecified, fmt::format("curl: {}", str));
  }

  static auto to_error(CURLMcode number) -> caf::error {
    if (number == CURLM_OK)
      return {};
    const auto* str = curl_multi_strerror(number);
    return caf::make_error(ec::unspecified, fmt::format("curl: {}", str));
  }

  static auto write_callback(void* ptr, size_t size, size_t nmemb,
                             void* user_data) -> size_t {
    TENZIR_ASSERT(size == 1);
    TENZIR_ASSERT(user_data != nullptr);
    const auto* data = reinterpret_cast<const std::byte*>(ptr);
    auto bytes = std::span<const std::byte>{data, nmemb};
    auto* chunks = reinterpret_cast<std::vector<chunk_ptr>*>(user_data);
    chunks->emplace_back(chunk::copy(bytes));
    return nmemb;
  }

public:
  static auto parse_url(const std::string& str) -> bool {
    auto* h = curl_url();
    auto result = curl_url_set(h, CURLUPART_URL, str.c_str(), 0) == CURLUE_OK;
    curl_url_cleanup(h);
    return result;
  }

  explicit curl() : easy_{curl_easy_init()}, multi_{curl_multi_init()} {
    auto err = set(CURLOPT_FOLLOWLOCATION, 1L);
    TENZIR_ASSERT(not err);
    err = set(CURLOPT_WRITEFUNCTION, write_callback);
    TENZIR_ASSERT(not err);
    // Enable all supported built-in compressions by setting the empty string.
    // This can always be overriden by manually setting the Accept-Encoding
    // header.
    err = set(CURLOPT_ACCEPT_ENCODING, "");
    TENZIR_ASSERT(not err);
    err = to_error(curl_multi_add_handle(multi_, easy_));
    TENZIR_ASSERT(not err);
  }

  curl(curl&) = delete;
  auto operator=(curl&) -> curl& = delete;
  curl(curl&&) = default;
  auto operator=(curl&&) -> curl& = default;

  ~curl() {
    if (headers_)
      curl_slist_free_all(headers_);
    curl_multi_remove_handle(multi_, easy_);
    curl_easy_cleanup(easy_);
    curl_multi_cleanup(multi_);
  }

  auto set(const curl_options& opts) -> caf::error {
    if (not opts.default_protocol.empty())
      if (auto err
          = set(CURLOPT_DEFAULT_PROTOCOL, opts.default_protocol.c_str()))
        return err;
    if (not opts.url.empty())
      if (auto err = set(CURLOPT_URL, opts.url.c_str()))
        return err;
    if (opts.default_protocol == "http" || opts.default_protocol == "https") {
      if (not opts.http.body.empty() || opts.http.method == "POST") {
        // Setting the size manually is important because curl would otherwise
        // use strlen to determine the body size. We may pass binary data at
        // some point.
        auto size = detail::narrow_cast<long>(opts.http.body.size());
        if (auto err = set(CURLOPT_POSTFIELDSIZE, size))
          return err;
        // Here is choice of CURLOPT_POSTFIELDS vs. CURLOPT_COPYPOSTFIELDS,
        // where the latter copies the input. We're copying now, but should
        // revisit this for large POST bodies.
        if (auto err = set(CURLOPT_COPYPOSTFIELDS, opts.http.body.c_str()))
          return err;
        // NB: setting body data via CURLOPT_POSTFIELDS implicitly sets the
        // Content-Type header to 'application/x-www-form-urlencoded' unless we
        // provide a Content-Type header that overrides it.
      }
      if (opts.http.method == "GET") {
        if (auto err = set(CURLOPT_HTTPGET, 1L))
          return err;
      } else if (opts.http.method == "POST") {
        if (auto err = set(CURLOPT_POST, 1L))
          return err;
      } else if (opts.http.method == "HEAD") {
        if (auto err = set(CURLOPT_NOBODY, 1L))
          return err;
      } else if (not opts.http.method.empty()) {
        if (auto err = set(CURLOPT_CUSTOMREQUEST, opts.http.method.c_str()))
          return err;
      }
      if (not opts.http.headers.empty()) {
        if (headers_) {
          curl_slist_free_all(headers_);
          headers_ = nullptr;
        }
        for (const auto& [header, value] : opts.http.headers) {
          auto str = header;
          str += ':';
          if (not value.empty()) {
            str += ' ';
            str += value;
          }
          headers_ = curl_slist_append(headers_, str.c_str());
        }
      }
      if (auto err = set(CURLOPT_HTTPHEADER, headers_))
        return err;
    }
    return {};
  }

  auto download(std::chrono::milliseconds timeout) -> generator<chunk_ptr> {
    // curl_easy_setopt(easy_, CURLOPT_VERBOSE, 1L);
    std::vector<chunk_ptr> chunks;
    auto err = set(CURLOPT_WRITEDATA, &chunks);
    TENZIR_ASSERT(not err);
    auto ms = detail::narrow_cast<int>(timeout.count());
    auto still_running = int{0};
    do {
      auto mc = curl_multi_perform(multi_, &still_running);
      if (mc == CURLM_OK && still_running != 0)
        mc = curl_multi_poll(multi_, nullptr, 0u, ms, nullptr);
      if (mc != CURLM_OK) {
        TENZIR_ERROR(to_error(mc));
        co_return;
      }
      if (chunks.empty())
        co_yield {};
      for (auto&& chunk : chunks)
        co_yield chunk;
      chunks.clear();
    } while (still_running != 0);
  }

private:
  auto set(CURLoption option, auto parameter) -> caf::error {
    return to_error(curl_easy_setopt(easy_, option, parameter));
  }

  CURL* easy_;
  CURLM* multi_;
  curl_slist* headers_;
};

struct connector_args {
  curl_options options;
  std::chrono::milliseconds poll_timeout = 1s;

  template <class Inspector>
  friend auto inspect(Inspector& f, connector_args& x) -> bool {
    return f.object(x)
      .pretty_name("connector_args")
      .fields(f.field("options", x.options),
              f.field("poll_timeout", x.poll_timeout));
  }
};

template <detail::string_literal Protocol>
class curl_loader final : public plugin_loader {
public:
  curl_loader() = default;

  explicit curl_loader(connector_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    auto make = [&ctrl](connector_args args) mutable -> generator<chunk_ptr> {
      auto handle = curl{};
      if (auto err = handle.set(args.options))
        ctrl.abort(err);
      co_yield {};
      for (auto&& chunk : handle.download(args.poll_timeout))
        co_yield chunk;
    };
    return make(args_);
  }

  auto name() const -> std::string override {
    return std::string{Protocol.str()};
  }

  auto default_parser() const -> std::string override {
    return "json";
  }

  friend auto inspect(auto& f, curl_loader& x) -> bool {
    return f.object(x)
      .pretty_name("curl_loader")
      .fields(f.field("args", x.args_));
  }

  // Remove once the base class has this function removed.
  auto to_string() const -> std::string override {
    return {};
  }

private:
  connector_args args_;
};

/// A key-value pair passed on the command line.
struct request_item {
  std::string_view type;
  std::string_view key;
  std::string_view value;
};

/// Parses a request item like HTTPie.
auto parse_request_item(std::string_view str) -> std::optional<request_item> {
  auto xs = detail::split(str, ":", "\\");
  if (xs.size() == 2)
    return request_item{.type = "header", .key = xs[0], .value = xs[1]};
  xs = detail::split(str, "==", "\\");
  if (xs.size() == 2)
    return request_item{.type = "url-param", .key = xs[0], .value = xs[1]};
  xs = detail::split(str, ":=", "\\");
  if (xs.size() == 2)
    return request_item{.type = "data-json", .key = xs[0], .value = xs[1]};
  xs = detail::split(str, "@", "\\");
  if (xs.size() == 2)
    return request_item{.type = "file-form", .key = xs[0], .value = xs[1]};
  xs = detail::split(str, "=@", "\\");
  if (xs.size() == 2)
    return request_item{.type = "file-data", .key = xs[0], .value = xs[1]};
  xs = detail::split(str, ":=@", "\\");
  if (xs.size() == 2)
    return request_item{.type = "file-data-json", .key = xs[0], .value = xs[1]};
  xs = detail::split(str, "=", "\\");
  if (xs.size() == 2)
    return request_item{.type = "data", .key = xs[0], .value = xs[1]};
  return {};
}

/// Parses a sequence of key-value pairs (= request items)
auto parse_http_options(std::vector<located<std::string>>& request_items)
  -> http_options {
  auto result = http_options{};
  auto body = record{};
  auto headers = std::map<std::string, std::string>{};
  for (const auto& request_item : request_items) {
    auto item = parse_request_item(request_item.inner);
    if (not item)
      diagnostic::error("failed to parse request item")
        .primary(request_item.source)
        .throw_();
    if (item->type == "header") {
      headers[std::string{item->key}] = std::string{item->value};
    } else if (item->type == "data") {
      result.method = "POST";
      body[std::string{item->key}] = std::string{item->value};
    } else {
      diagnostic::error("unsupported request item type")
        .primary(request_item.source)
        .throw_();
    }
    if (not body.empty()) {
      // We're currently only supporting JSON. In the future we're going to
      // support -f for form-encoded data as well.
      auto json_body = to_json(body, {.oneline = true});
      TENZIR_ASSERT_CHEAP(json_body);
      result.body = std::move(*json_body);
      result.headers["Accept"] = "application/json, */*";
      result.headers["Content-Type"] = "application/json";
    }
    // User-provided headers always have precedence, which is why we process
    // them last, after we added automatically generated ones previously.
    for (auto& [header, value] : headers)
      result.headers[header] = std::move(value);
  }
  return result;
}

template <detail::string_literal Protocol>
class plugin final : public virtual loader_plugin<curl_loader<Protocol>> {
  /// Auto-completes a scheme-less URL with the schem from this plugin.
  static auto auto_complete(std::string_view url) -> std::string {
    if (url.find("://") != std::string_view::npos)
      return std::string{url};
    return fmt::format("{}://{}", Protocol.str(), url);
  }

public:
  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto args = connector_args{
      .options = {.default_protocol = name()},
    };
    auto make = [&]() {
      return std::make_unique<curl_loader<Protocol>>(std::move(args));
    };
    // For HTTP and HTTPS the desired CLI UX is HTTPie:
    //
    //     [<method>] <url> [<item>..]
    //
    // Please see `man http` for an explanation of the desired outcome.
    if (name() == "http" || name() == "https") {
      // Collect all arguments first until `argument_parser` becomes mightier.
      auto items = std::vector<located<std::string>>{};
      while (auto arg = p.accept_shell_arg())
        items.push_back(std::move(*arg));
      if (items.empty())
        diagnostic::error("no URL provided").throw_();
      // No ambiguity, just go with <url>.
      if (items.size() == 1) {
        args.options.url = std::move(items[0].inner);
        return make();
      }
      TENZIR_ASSERT(items.size() >= 2);
      // Try <method> <url> [<item>..]
      auto method_regex = std::regex{"[a-zA-Z]+"};
      if (std::regex_match(items[0].inner, method_regex)) {
        TENZIR_DEBUG("detected syntax: <method> <url> [<item>..]");
        // FIXME: find a strategy to deal with some false positives here, e.g.,
        // "localhost".
        auto method = std::move(items[0].inner);
        args.options.url = auto_complete(items[1].inner);
        items.erase(items.begin());
        items.erase(items.begin());
        args.options.http = parse_http_options(items);
        args.options.http.method = std::move(method);
        return make();
      }
      TENZIR_DEBUG("trying last possible syntax: <url> <item> [<item>..]");
      args.options.url = auto_complete(items[0].inner);
      items.erase(items.begin());
      args.options.http = parse_http_options(items);
      return make();
    } else {
      auto parser = argument_parser{
        name(),
        fmt::format("https://docs.tenzir.com/docs/connectors/{}", name())};
      parser.add(args.options.url, "<url>");
      parser.parse(p);
    }
    return make();
  }

  auto name() const -> std::string override {
    return std::string{Protocol.str()};
  }
};

} // namespace

// Available protocol names according to the documentation at
// https://curl.se/libcurl/c/CURLOPT_DEFAULT_PROTOCOL.html are: dict, file, ftp,
// ftps, gopher, http, https, imap, imaps, ldap, ldaps, pop3, pop3s, rtsp, scp,
// sftp, smb, smbs, smtp, smtps, telnet, tftp

using ftp = plugin<"ftp">;
using ftps = plugin<"ftps">;
using http = plugin<"http">;
using https = plugin<"https">;

} // namespace tenzir::plugins

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ftp)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ftps)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::http)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::https)
