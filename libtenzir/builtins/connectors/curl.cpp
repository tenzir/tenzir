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
#include <tenzir/curl.hpp>
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
      for (auto&& chunk : handle.download(args.poll_timeout)) {
        if (not chunk) {
          diagnostic::error("failed to download {}", args.options.url)
            .hint(fmt::format("{}", chunk.error()))
            .emit(ctrl.diagnostics());
          co_return;
        }
        co_yield *chunk;
      }
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

private:
  connector_args args_;
};

/// A key-value pair passed on the command line.
struct request_item {
  std::string_view type;
  std::string key;
  std::string value;
};

/// Parses a request item like HTTPie.
auto parse_request_item(std::string_view str) -> std::optional<request_item> {
  auto xs = detail::split_escaped(str, ":=@", "\\", 1);
  if (xs.size() == 2)
    return request_item{.type = "file-data-json", .key = xs[0], .value = xs[1]};
  xs = detail::split_escaped(str, ":=", "\\", 1);
  if (xs.size() == 2)
    return request_item{.type = "data-json", .key = xs[0], .value = xs[1]};
  xs = detail::split_escaped(str, "==", "\\", 1);
  if (xs.size() == 2)
    return request_item{.type = "url-param", .key = xs[0], .value = xs[1]};
  xs = detail::split_escaped(str, "=@", "\\", 1);
  if (xs.size() == 2)
    return request_item{.type = "file-data", .key = xs[0], .value = xs[1]};
  xs = detail::split_escaped(str, "@", "\\", 1);
  if (xs.size() == 2)
    return request_item{.type = "file-form", .key = xs[0], .value = xs[1]};
  xs = detail::split_escaped(str, "=", "\\", 1);
  if (xs.size() == 2)
    return request_item{.type = "data", .key = xs[0], .value = xs[1]};
  xs = detail::split_escaped(str, ":", "\\", 1);
  if (xs.size() == 2)
    return request_item{.type = "header", .key = xs[0], .value = xs[1]};
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
    } else if (item->type == "data-json") {
      result.method = "POST";
      auto data = from_json(item->value);
      if (not data)
        diagnostic::error("invalid JSON value")
          .primary(request_item.source)
          .throw_();
      body[std::string{item->key}] = std::move(*data);
    } else {
      diagnostic::error("unsupported request item type")
        .primary(request_item.source)
        .throw_();
    }
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
  return result;
}

template <detail::string_literal Protocol>
class plugin final : public virtual loader_plugin<curl_loader<Protocol>> {
public:
  static auto protocol() -> std::string {
    return std::string{Protocol.str()};
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    return std::make_unique<curl_loader<Protocol>>(parse_args(p));
  }

  auto name() const -> std::string override {
    return protocol();
  }

private:
  /// Auto-completes a scheme-less URL with the scheme from this plugin.
  static auto auto_complete(std::string_view url) -> std::string {
    if (url.find("://") != std::string_view::npos)
      return std::string{url};
    return fmt::format("{}://{}", Protocol.str(), url);
  }

  static auto parse_args(parser_interface& p) -> connector_args {
    auto result = connector_args{};
    result.options.default_protocol = protocol();
    // For HTTP and HTTPS the desired CLI UX is HTTPie:
    //
    //     [<method>] <url> [<item>..]
    //
    // Please see `man http` for an explanation of the desired outcome.
    if (protocol() == "http" || protocol() == "https") {
      // Collect all arguments first until `argument_parser` becomes mightier.
      auto items = std::vector<located<std::string>>{};
      while (auto arg = p.accept_shell_arg()) {
        // Process options here manually until argument_parser becomes more
        // powerful.
        if (arg->inner == "-v" || arg->inner == "--verbose")
          result.options.verbose = true;
        else
          items.push_back(std::move(*arg));
      }
      if (items.empty())
        diagnostic::error("no URL provided").throw_();
      // No ambiguity, just go with <url>.
      if (items.size() == 1) {
        result.options.url = std::move(items[0].inner);
        return result;
      }
      TENZIR_ASSERT(items.size() >= 2);
      // Try <method> <url> [<item>..]
      auto method_regex = std::regex{"[a-zA-Z]+"};
      if (std::regex_match(items[0].inner, method_regex)) {
        TENZIR_DEBUG("detected syntax: <method> <url> [<item>..]");
        // FIXME: find a strategy to deal with some false positives here, e.g.,
        // "localhost".
        auto method = std::move(items[0].inner);
        result.options.url = auto_complete(items[1].inner);
        items.erase(items.begin());
        items.erase(items.begin());
        result.options.http = parse_http_options(items);
        result.options.http.method = std::move(method);
        return result;
      }
      TENZIR_DEBUG("trying last possible syntax: <url> <item> [<item>..]");
      result.options.url = auto_complete(items[0].inner);
      items.erase(items.begin());
      result.options.http = parse_http_options(items);
      return result;
    } else {
      auto parser = argument_parser{
        protocol(),
        fmt::format("https://docs.tenzir.com/docs/connectors/{}", protocol())};
      parser.add("-v,--verbose", result.options.verbose);
      parser.add(result.options.url, "<url>");
      parser.parse(p);
    }
    return result;
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
