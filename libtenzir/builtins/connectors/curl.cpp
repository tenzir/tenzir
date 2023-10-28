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
#include <tenzir/detail/string.hpp>
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

template <detail::string_literal Protocol>
class curl_saver final : public plugin_saver {
public:
  curl_saver() = default;

  explicit curl_saver(connector_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl, std::optional<printer_info>)
    -> caf::expected<std::function<void(chunk_ptr)>> override {
    // We only use a pointer here to be able to move the handle into the lambda
    // state.
    auto handle = std::make_shared<curl>();
    if (auto err = handle->set(args_.options))
      return err;
    // The saver gets its input from the upstream operator, so we ignore any
    // user-provided request body.
    if (not args_.options.http.body.data.empty()) {
      const auto* ptr
        = reinterpret_cast<const char*>(args_.options.http.body.data.data());
      auto size = args_.options.http.body.data.size();
      diagnostic::warning("ignoring non-empty HTTP request body arguments")
        .note("{}", detail::byte_escape(std::string_view{ptr, size}))
        .emit(ctrl.diagnostics());
    }
    return [&ctrl, args = args_,
            handle = std::move(handle)](chunk_ptr chunk) mutable {
      if (!chunk || chunk->size() == 0)
        return;
      // TODO: make this smarter and provide the contentt type. We currently
      // blindly assume application/json.
      if (auto err = handle->upload(as_bytes(chunk))) {
        diagnostic::error("failed to upload chunk ({} bytes) to {}",
                          chunk->size(), args.options.url)
          .hint("{}", err)
          .emit(ctrl.diagnostics());
        return;
      }
    };
  }

  auto name() const -> std::string override {
    return std::string{Protocol.str()};
  }

  auto default_printer() const -> std::string override {
    return "json";
  }

  auto is_joining() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, curl_saver& x) -> bool {
    return f.object(x)
      .pretty_name("curl_saver")
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
auto parse_http_options(http_options& opts,
                        std::vector<located<std::string>>& request_items)
  -> void {
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
      if (opts.method.empty())
        opts.method = "POST";
      body[std::string{item->key}] = std::string{item->value};
    } else if (item->type == "data-json") {
      if (opts.method.empty())
        opts.method = "POST";
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
  if (opts.body.content_type.empty()
      or opts.body.content_type.starts_with("application/json")) {
    if (not body.empty()) {
      auto json_body = to_json(body, {.oneline = true});
      TENZIR_ASSERT_CHEAP(json_body);
      auto bytes = as_bytes(*json_body);
      opts.body.data = {bytes.begin(), bytes.end()};
      opts.body.content_type = "application/json";
      opts.headers["Accept"] = "application/json, */*";
    }
  } else if (opts.body.content_type.starts_with("application/"
                                                "x-www-form-urlencoded")) {
    auto url_encoded = curl{}.escape(flatten(body));
    TENZIR_DEBUG("urlencoded request body: {}", url_encoded);
    auto bytes = as_bytes(url_encoded);
    opts.body.data = {bytes.begin(), bytes.end()};
  } else {
    diagnostic::error("could not encode request body with content type {}",
                      opts.body.content_type)
      .throw_();
  }
  // User-provided headers always have precedence, which is why we process
  // them last, after we added automatically generated ones previously.
  for (auto& [header, value] : headers)
    opts.headers[header] = std::move(value);
}

template <detail::string_literal Protocol>
class plugin final : public virtual loader_plugin<curl_loader<Protocol>>,
                     public virtual saver_plugin<curl_saver<Protocol>> {
public:
  static auto protocol() -> std::string {
    return std::string{Protocol.str()};
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    return std::make_unique<curl_loader<Protocol>>(parse_args(p));
  }

  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    return std::make_unique<curl_saver<Protocol>>(parse_args(p));
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
        if (arg->inner == "-v" || arg->inner == "--verbose") {
          result.options.verbose = true;
        } else if (arg->inner == "-j" || arg->inner == "--json") {
          result.options.http.body.content_type = "application/json";
          result.options.http.headers["Accept"] = "application/json";
        } else if (arg->inner == "-f" || arg->inner == "--form") {
          result.options.http.body.content_type
            = "application/x-www-form-urlencoded";
        } else {
          items.push_back(std::move(*arg));
        }
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
        // "localhost" should be interepreted as URL and not HTTP method.
        auto method = std::move(items[0].inner);
        result.options.url = auto_complete(items[1].inner);
        items.erase(items.begin());
        items.erase(items.begin());
        result.options.http.method = std::move(method);
        parse_http_options(result.options.http, items);
        return result;
      }
      TENZIR_DEBUG("trying last possible syntax: <url> <item> [<item>..]");
      result.options.url = auto_complete(items[0].inner);
      items.erase(items.begin());
      parse_http_options(result.options.http, items);
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
