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

#include <caf/actor_system_config.hpp>

#include <string_view>
#include <utility>

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
  if (auto err = apply(args.http_opts.items, result)) {
    return err;
  }
  return result;
}

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
    co_yield {};
    auto args = args_;
    args.transfer_opts.ssl.update_cacert(ctrl);
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
    for (auto&& chunk : tx.download_chunks()) {
      if (not chunk) {
        diagnostic::error("failed to download {}", args.url)
          .hint(fmt::format("{}", chunk.error()))
          .emit(ctrl.diagnostics());
        co_return;
      }
      co_yield *chunk;
    }
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto detached() const -> bool override {
    return true;
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
    co_yield {};
    // TODO: Clean this up.
    auto args = args_;
    args.transfer_opts.ssl.update_cacert(ctrl);
    auto req = make_request(args_);
    if (not req) {
      diagnostic::error("failed to construct HTTP request")
        .note("{}", req.error())
        .emit(ctrl.diagnostics());
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
    }
    auto tx = transfer(args_.transfer_opts);
    if (auto err = tx.prepare(std::move(*req))) {
      diagnostic::error("failed to prepare HTTP request")
        .note("{}", err)
        .emit(ctrl.diagnostics());
    }
    for (const auto& chunk : input) {
      if (not chunk || chunk->size() == 0) {
        co_yield {};
        continue;
      }
      if (auto err = tx.prepare(chunk)) {
        diagnostic::error("failed to prepare transfer")
          .note("chunk size: {}", chunk->size())
          .note("{}", err)
          .emit(ctrl.diagnostics());
      }
      if (auto err = tx.perform()) {
        diagnostic::error("failed to upload chunk to {}", args.url)
          .note("{}", err)
          .emit(ctrl.diagnostics());
      }
    }
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto detached() const -> bool override {
    return true;
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

auto parse_http_args(const std::string& name,
                     const operator_factory_plugin::invocation& inv,
                     session ctx) -> failure_or<connector_args> {
  auto url = located<std::string>{};
  auto body_data = std::optional<located<record>>{};
  auto params = std::optional<located<record>>{};
  auto headers = std::optional<located<record>>{};
  auto form = std::optional<location>{};
  auto method = std::optional<std::string>{};
  auto args = connector_args{};
  args.transfer_opts.default_protocol = "https";
  auto parser = argument_parser2::operator_(name);
  parser.positional("url", url);
  parser.named("params", params);
  parser.named("headers", headers);
  parser.named("method", method);
  if (name == "load_http") {
    parser.named("data", body_data);
    parser.named("form", form);
    parser.named("chunked", args.http_opts.chunked);
    parser.named("multipart", args.http_opts.multipart);
  }
  args.transfer_opts.ssl.add_tls_options(parser);
  parser.named("_verbose", args.transfer_opts.verbose);
  TRY(parser.parse(inv, ctx));
  TRY(args.transfer_opts.ssl.validate(url, ctx));
  args.url = std::move(url.inner);
  if (form) {
    args.http_opts.form = true;
  }
  if (body_data) {
    for (auto& [key, value] : body_data->inner) {
      auto str = to_json(value);
      TENZIR_ASSERT(str);
      args.http_opts.items.emplace_back(tenzir::http::request_item::data_json,
                                        std::move(key), std::move(*str));
    }
  }
  if (params) {
    for (auto& [name, value] : params->inner) {
      // TODO: What about other types?
      auto* str = try_as<std::string>(&value);
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
      auto* str = try_as<std::string>(&value);
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
  if (method) {
    args.http_opts.method = std::move(*method);
  }
  return args;
}

class load_http_plugin final
  : public virtual operator_plugin2<load_http_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    diagnostic::warning(
      "`load_http` is deprecated and will be removed in a future release")
      .hint("use `from` or `from_http` instead")
      .primary(inv.self.get_location())
      .emit(ctx);
    TRY(auto args, parse_http_args("load_http", inv, ctx));
    return std::make_unique<load_http_operator>(std::move(args));
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
      .schemes = {"http", "https"},
      .default_format = plugins::find<operator_factory_plugin>("write_ndjson"),
    };
  }
};

auto parse_ftp_args(std::string name,
                    const operator_factory_plugin::invocation& inv, session ctx)
  -> failure_or<connector_args> {
  auto args = connector_args{};
  auto parser = argument_parser2::operator_(std::move(name));
  parser.positional("url", args.url);
  args.transfer_opts.ssl.add_tls_options(parser);
  TRY(parser.parse(inv, ctx));
  if (not args.url.starts_with("ftp://")
      and not args.url.starts_with("ftps://")) {
    args.url.insert(0, "ftp://");
  }
  TRY(args.transfer_opts.ssl.validate(args.url, location::unknown, ctx));
  return args;
}

class load_ftp_plugin final
  : public virtual operator_plugin2<load_http_operator> {
public:
  auto name() const -> std::string override {
    return "load_ftp";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(auto args, parse_ftp_args(name(), inv, ctx));
    return std::make_unique<load_http_operator>(std::move(args));
  }

  auto load_properties() const -> load_properties_t override {
    return {
      .schemes = {"ftp", "ftps"},
    };
  }
};

class save_ftp_plugin final
  : public virtual operator_plugin2<save_http_operator> {
public:
  auto name() const -> std::string override {
    return "save_ftp";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(auto args, parse_ftp_args(name(), inv, ctx));
    return std::make_unique<save_http_operator>(std::move(args));
  }

  auto save_properties() const -> save_properties_t override {
    return {
      .schemes = {"ftp", "ftps"},
    };
  }
};

} // namespace

} // namespace tenzir::plugins

TENZIR_REGISTER_PLUGIN(tenzir::plugins::load_http_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::save_http_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::load_ftp_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::save_ftp_plugin)
