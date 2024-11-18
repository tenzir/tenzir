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
#include <tenzir/http.hpp>
#include <tenzir/location.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/transfer.hpp>

#include <filesystem>
#include <regex>
#include <system_error>

using namespace std::chrono_literals;

namespace tenzir::plugins {

namespace {

struct http_options {
  bool json;
  bool form;
  bool chunked;
  bool multipart;
  std::string method;
  std::vector<http::request_item> items;

  friend auto inspect(auto& f, http_options& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.http_options")
      .fields(f.field("json", x.json), f.field("form", x.form),
              f.field("chunked", x.chunked), f.field("multipart", x.multipart),
              f.field("method", x.method), f.field("items", x.items));
  }
};

struct connector_args {
  std::string url;
  transfer_options transfer_opts;
  http_options http_opts;

  friend auto inspect(auto& f, connector_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.connector_args")
      .fields(f.field("url", x.url), f.field("transfer_opts", x.transfer_opts),
              f.field("http_opts", x.http_opts));
  }
};

auto make_request(const connector_args& args) -> caf::expected<http::request> {
  auto result = http::request{};
  // Set URL.
  result.uri = args.url;
  // Set method.
  result.method = args.http_opts.method;
  if (args.http_opts.json) {
    result.headers.emplace_back("Accept", "application/json");
    if (auto* header = result.header("Content-Type")) {
      TENZIR_DEBUG("overwriting Content-Type to application/json (was: {})",
                   header->value);
      header->value = "application/json";
    } else {
      result.headers.emplace_back("Content-Type", "application/json");
    }
  } else if (args.http_opts.form) {
    result.headers.emplace_back("Content-Type",
                                "application/x-www-form-urlencoded");
  }
  if (args.http_opts.chunked) {
    result.headers.emplace_back("Transfer-Encoding", "chunked");
  }
  if (auto err = apply(args.http_opts.items, result))
    return err;
  return result;
}

template <detail::string_literal Protocol>
class curl_loader final : public plugin_loader {
public:
  curl_loader() = default;

  explicit curl_loader(connector_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    auto make = [](operator_control_plane& ctrl,
                   connector_args args) mutable -> generator<chunk_ptr> {
      auto tx = transfer{args.transfer_opts};
      auto req = make_request(args);
      if (not req) {
        diagnostic::error("failed to construct HTTP request")
          .note("{}", req.error())
          .emit(ctrl.diagnostics());
        co_return;
      }
      if (auto err = tx.prepare(*req)) {
        diagnostic::error("failed to prepare HTTP request")
          .note("{}", err)
          .emit(ctrl.diagnostics());
        co_return;
      }
      if (args.http_opts.multipart) {
        if (req->body.empty()) {
          diagnostic::warning("ignoring request to send multipart message")
            .note("HTTP request body is empty")
            .emit(ctrl.diagnostics());
        } else {
          // Move body over to MIME part.
          auto& easy = tx.handle();
          auto mime = curl::mime{easy};
          auto part = mime.add();
          part.data(as_bytes(req->body));
          if (auto* header = req->header("Content-Type")) {
            part.type(header->value);
            easy.set_http_header("Content-Type", "multipart/form-data");
          }
          req->body.clear();
          auto code = easy.set(std::move(mime));
          if (code != curl::easy::code::ok) {
            diagnostic::error("failed to construct HTTP request")
              .note("{}", req.error())
              .emit(ctrl.diagnostics());
            co_return;
          }
        }
      }
      co_yield {};
      for (auto&& chunk : tx.download_chunks()) {
        if (not chunk) {
          diagnostic::error("failed to download {}", args.url)
            .hint(fmt::format("{}", chunk.error()))
            .emit(ctrl.diagnostics());
          co_return;
        }
        co_yield *chunk;
      }
    };
    return make(ctrl, args_);
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
    auto req = make_request(args_);
    if (not req) {
      diagnostic::error("failed to construct HTTP request")
        .note("{}", req.error())
        .emit(ctrl.diagnostics());
      return req.error();
    }
    // We're trying to accommodate the most common scenario of getting JSON to
    // be submitted via a POST request.
    if (req->method.empty()) {
      req->method = "POST";
    }
    if (not req->body.empty()) {
      diagnostic::error("found {}-byte HTTP request body", req->body.size())
        .note("cannot use request body in HTTP saver")
        .note("pipeline input is the only request body")
        .hint("remove arguments that create a request body")
        .emit(ctrl.diagnostics());
      return caf::make_error(ec::invalid_argument, "bogus operator arguments");
    }
    auto tx = std::make_shared<transfer>(args_.transfer_opts);
    if (auto err = tx->prepare(std::move(*req))) {
      diagnostic::error("failed to prepare HTTP request")
        .note("{}", err)
        .emit(ctrl.diagnostics());
      return err;
    }
    return [&ctrl, args = args_, tx](chunk_ptr chunk) mutable {
      if (!chunk || chunk->size() == 0)
        return;
      if (auto err = tx->prepare(chunk)) {
        diagnostic::error("failed to prepare transfer")
          .note("chunk size: {}", chunk->size())
          .note("{}", err)
          .emit(ctrl.diagnostics());
        return;
      }
      if (auto err = tx->perform()) {
        diagnostic::error("failed to upload chunk to {}", args.url)
          .note("{}", err)
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
    result.transfer_opts.default_protocol = protocol();
    // For HTTP and HTTPS the desired CLI UX is HTTPie:
    //
    //     [<method>] <url> [<item>..]
    //
    // Please see `man http` for an explanation of the desired outcome.
    if (protocol() == "http" || protocol() == "https") {
      // Collect all arguments first until `argument_parser` becomes mightier.
      auto args = std::vector<located<std::string>>{};
      // Process options here manually until argument_parser becomes more
      // powerful.
      while (auto arg = p.accept_shell_arg()) {
        if (arg->inner == "-v" || arg->inner == "--verbose") {
          result.transfer_opts.verbose = true;
        } else if (arg->inner == "-j" || arg->inner == "--json") {
          result.http_opts.json = true;
        } else if (arg->inner == "-f" || arg->inner == "--form") {
          result.http_opts.form = true;
        } else if (arg->inner == "--chunked") {
          result.http_opts.chunked = true;
        } else if (arg->inner == "--multipart") {
          result.http_opts.multipart = true;
          // TODO: factor these TLS options in the future, as they apply to many
          // connectors, such as email.
        } else if (arg->inner == "-P" || arg->inner == "--skip-peer-verification") {
          result.transfer_opts.skip_peer_verification = true;
        } else if (arg->inner == "-H" || arg->inner == "--skip-hostname-verification") {
          result.transfer_opts.skip_hostname_verification = true;
        } else {
          args.push_back(std::move(*arg));
        }
      }
      TENZIR_DEBUG("parsed shell arguments:");
      for (auto i = 0u; i < args.size(); ++i) {
        TENZIR_DEBUG("- args[{}] = {}", i, args[i].inner);
      }
      if (args.empty())
        diagnostic::error("no URL provided").throw_();
      // No ambiguity, just go with <url>.
      if (args.size() == 1) {
        result.url = auto_complete(args[0].inner);
        return result;
      }
      TENZIR_ASSERT(args.size() >= 2);
      // Try <method> <url> [<item>..]
      auto method_regex = std::regex{"[a-zA-Z]+"};
      if (std::regex_match(args[0].inner, method_regex)) {
        // FIXME: find a strategy to deal with some false positives here, e.g.,
        // "localhost" should be interepreted as URL and not HTTP method.
        TENZIR_DEBUG("detected syntax: <method> <url> [<item>..]");
        result.http_opts.method = std::move(args[0].inner);
        result.url = auto_complete(args[1].inner);
        args.erase(args.begin());
        args.erase(args.begin());
        for (auto& arg : args) {
          if (auto item = http::request_item::parse(arg.inner))
            result.http_opts.items.push_back(std::move(*item));
          else
            diagnostic::error("invalid HTTP request item")
              .primary(arg.source)
              .note("{}", arg.inner)
              .throw_();
        }
        return result;
      }
      TENZIR_DEBUG("trying last possible syntax: <url> <item> [<item>..]");
      result.url = auto_complete(args[0].inner);
      args.erase(args.begin());
      for (auto& arg : args) {
        if (auto item = http::request_item::parse(arg.inner))
          result.http_opts.items.push_back(std::move(*item));
        else
          diagnostic::error("invalid HTTP request item")
            .primary(arg.source)
            .note("{}", arg.inner)
            .throw_();
      }
      return result;
    } else {
      auto parser = argument_parser{
        protocol(),
        fmt::format("https://docs.tenzir.com/connectors/{}", protocol())};
      parser.add("-v,--verbose", result.transfer_opts.verbose);
      parser.add(result.url, "<url>");
      parser.parse(p);
    }
    return result;
  }
};

// Available protocol names according to the documentation at
// https://curl.se/libcurl/c/CURLOPT_DEFAULT_PROTOCOL.html are: dict, file, ftp,
// ftps, gopher, http, https, imap, imaps, ldap, ldaps, pop3, pop3s, rtsp, scp,
// sftp, smb, smbs, smtp, smtps, telnet, tftp

using ftp = plugin<"ftp">;
using ftps = plugin<"ftps">;
using http = plugin<"http">;
using https = plugin<"https">;

class load_http_operator final : public crtp_operator<load_http_operator> {
public:
  load_http_operator() = default;

  explicit load_http_operator(connector_args args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "tql2.load_http";
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    // TODO: Clean this up.
    auto loader = curl_loader<"TODO: not using this">{args_};
    auto gen = loader.instantiate(ctrl);
    TENZIR_ASSERT(gen);
    for (auto chunk : *gen) {
      co_yield std::move(chunk);
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, load_http_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  connector_args args_;
};

class save_http_operator final : public crtp_operator<save_http_operator> {
public:
  save_http_operator() = default;

  explicit save_http_operator(connector_args args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "tql2.save_http";
  }

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    // TODO: Clean this up.
    auto saver = curl_saver<"TODO: not using this">{args_};
    auto func = saver.instantiate(ctrl, std::nullopt);
    if (not func) {
      diagnostic::error(func.error()).emit(ctrl.diagnostics());
      co_return;
    }
    for (auto chunk : input) {
      (*func)(std::move(chunk));
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, save_http_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  connector_args args_;
};

auto parse_http_args(std::string name,
                     const operator_factory_plugin::invocation& inv,
                     session ctx) -> failure_or<connector_args> {
  auto url = std::string{};
  auto method = std::optional<std::string>{};
  auto params = std::optional<located<record>>{};
  auto headers = std::optional<located<record>>{};
  argument_parser2::operator_(std::move(name))
    .add(url, "<url>")
    .add("method", method)
    .add("params", params)
    .add("headers", headers)
    .parse(inv, ctx)
    .ignore();
  auto args = connector_args{};
  args.url = std::move(url);
  if (method) {
    args.http_opts.method = *method;
  }
  if (params) {
    for (auto& [name, value] : params->inner) {
      // TODO: What about other types?
      auto str = try_as<std::string>(&value);
      if (not str) {
        diagnostic::error("expected `string` for parameter `{}`", name)
          .primary(*params)
          .emit(ctx);
        continue;
      }
      args.http_opts.items.emplace_back(tenzir::http::request_item::url_param,
                                        std::move(name), std::move(*str));
    }
  }
  if (headers) {
    for (auto& [name, value] : headers->inner) {
      // TODO: What about other types?
      auto str = try_as<std::string>(&value);
      if (not str) {
        diagnostic::error("expected `string` for header `{}`", name)
          .primary(*headers)
          .emit(ctx);
        continue;
      }
      args.http_opts.items.emplace_back(tenzir::http::request_item::header,
                                        std::move(name), std::move(*str));
    }
  }
  return args;
}

class load_http_plugin final
  : public virtual operator_plugin2<load_http_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(auto args, parse_http_args("load_http", inv, ctx));
    return std::make_unique<load_http_operator>(std::move(args));
  }

  auto load_properties() const -> load_properties_t override {
    return {
      .schemes = {"http", "https", "ftp", "ftps"},
    };
  }
};

class save_http_plugin final
  : public virtual operator_plugin2<save_http_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(auto args, parse_http_args("save_http", inv, ctx));
    return std::make_unique<save_http_operator>(std::move(args));
  }

  auto save_properties() const -> save_properties_t override {
    return {
      .schemes = {"http", "https", "ftp", "ftps"},
    };
  }
};

} // namespace

} // namespace tenzir::plugins

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ftp)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ftps)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::http)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::https)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::load_http_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::save_http_plugin)
