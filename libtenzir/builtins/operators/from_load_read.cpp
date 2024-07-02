//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <tenzir/detail/loader_saver_resolver.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/prepend_token.hpp>
#include <tenzir/tql/fwd.hpp>
#include <tenzir/tql/parser.hpp>

namespace tenzir::plugins::from {
namespace {

class load_operator final : public crtp_operator<load_operator> {
public:
  load_operator() = default;

  explicit load_operator(std::unique_ptr<plugin_loader> loader)
    : loader_{std::move(loader)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> caf::expected<generator<chunk_ptr>> {
    if (auto result = loader_->instantiate(ctrl)) {
      return std::move(*result);
    }
    return caf::make_error(ec::silent, "could not instantiate loader");
  }

  auto detached() const -> bool override {
    return true;
  }

  auto name() const -> std::string override {
    return "load";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter, (void)order;
    return do_not_optimize(*this);
  }

  auto internal() const -> bool override {
    return loader_->internal();
  }

  friend auto inspect(auto& f, load_operator& x) -> bool {
    return plugin_inspect(f, x.loader_);
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

protected:
  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override {
    if (input.is<void>()) {
      return tag_v<chunk_ptr>;
    }
    // TODO: Fuse this check with crtp_operator::instantiate()
    return caf::make_error(ec::type_clash,
                           fmt::format("'{}' does not accept {} as input",
                                       name(), operator_type_name(input)));
  }

private:
  std::unique_ptr<plugin_loader> loader_;
};

class read_operator final : public crtp_operator<read_operator> {
public:
  read_operator() = default;

  explicit read_operator(std::unique_ptr<plugin_parser> parser)
    : parser_{std::move(parser)} {
  }

  auto name() const -> std::string override {
    return "read";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    if (order == event_order::ordered) {
      return do_not_optimize(*this);
    }
    // TODO: We could also propagate `where #schema == "..."` to the parser.
    auto parser_opt = parser_->optimize(order);
    if (not parser_opt) {
      return do_not_optimize(*this);
    }
    return optimize_result{
      std::nullopt,
      event_order::ordered,
      std::make_unique<read_operator>(std::move(parser_opt)),
    };
  }

  friend auto inspect(auto& f, read_operator& x) -> bool {
    return plugin_inspect(f, x.parser_);
  }

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> caf::expected<generator<table_slice>> {
    auto parser = parser_->instantiate(std::move(input), ctrl);
    if (not parser) {
      return caf::make_error(ec::silent, "could not instantiate parser");
    }
    return std::move(*parser);
  }

protected:
  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override {
    if (input.is<chunk_ptr>()) {
      return tag_v<table_slice>;
    }
    // TODO: Fuse this check with crtp_operator::instantiate()
    return caf::make_error(ec::type_clash,
                           fmt::format("'{}' does not accept {} as input",
                                       name(), operator_type_name(input)));
  }

private:
  std::unique_ptr<plugin_parser> parser_;
};

[[noreturn]] void
throw_loader_not_found(located<std::string_view> x, bool use_uri_schemes) {
  auto available = std::vector<std::string>{};
  for (auto const* p : plugins::get<loader_parser_plugin>()) {
    if (use_uri_schemes) {
      for (auto uri_scheme : p->supported_uri_schemes()) {
        available.push_back(std::move(uri_scheme));
      }
    } else {
      available.push_back(p->name());
    }
  }
  if (use_uri_schemes) {
    diagnostic::error("loader for `{}` scheme could not be found", x.inner)
      .primary(x.source)
      .hint("must be one of {}", fmt::join(available, ", "))
      .docs("https://docs.tenzir.com/connectors")
      .throw_();
  }
  diagnostic::error("loader `{}` could not be found", x.inner)
    .primary(x.source)
    .hint("must be one of {}", fmt::join(available, ", "))
    .docs("https://docs.tenzir.com/connectors")
    .throw_();
}

[[noreturn]] void throw_parser_not_found(located<std::string_view> x) {
  auto available = std::vector<std::string>{};
  for (auto p : plugins::get<parser_parser_plugin>()) {
    available.push_back(p->name());
  }
  diagnostic::error("parser `{}` could not be found", x.inner)
    .primary(x.source)
    .hint("must be one of {}", fmt::join(available, ", "))
    .docs("https://docs.tenzir.com/formats")
    .throw_();
}

auto get_loader(parser_interface& p, const char* usage, const char* docs)
  -> std::pair<std::unique_ptr<plugin_loader>, located<std::string>> {
  auto l_name = p.accept_shell_arg();
  if (not l_name) {
    diagnostic::error("expected loader name")
      .primary(p.current_span())
      .usage(usage)
      .docs(docs)
      .throw_();
  }
  auto [loader, name, path, is_uri] = detail::resolve_loader(p, *l_name);
  if (not loader) {
    throw_loader_not_found(name, is_uri);
  }
  return {std::move(loader), std::move(path)};
}

class from_plugin final : public virtual operator_parser_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto name() const -> std::string override {
    return "from";
  };

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto usage = "from <loader> <args>... [read <parser> <args>...]";
    auto docs = "https://docs.tenzir.com/operators/from";
    if (const auto peek_tcp = p.peek_shell_arg();
        peek_tcp
        and (peek_tcp->inner == "tcp"
             or peek_tcp->inner.starts_with("tcp://"))) {
      if (peek_tcp->inner == "tcp") {
        (void)p.accept_identifier();
      }
      const auto* accept_plugin = plugins::find_operator("tcp-listen");
      if (not accept_plugin) {
        diagnostic::error("`tcp-listen` plugin is required").throw_();
      }
      return accept_plugin->parse_operator(p);
    }
    auto q = until_keyword_parser{"read", p};
    auto [loader, loader_path] = get_loader(q, usage, docs);
    TENZIR_DIAG_ASSERT(loader);
    TENZIR_DIAG_ASSERT(q.at_end());
    auto decompress = operator_ptr{};
    auto parser = std::unique_ptr<plugin_parser>{};
    if (p.at_end()) {
      std::tie(decompress, parser)
        = detail::resolve_parser(loader_path, loader->default_parser());
    } else {
      decompress = detail::resolve_decompressor(loader_path);
      auto read = p.accept_identifier();
      TENZIR_DIAG_ASSERT(read && read->name == "read");
      auto p_name = p.accept_shell_arg();
      if (!p_name) {
        diagnostic::error("expected parser name")
          .primary(p.current_span())
          .note(usage)
          .docs(docs)
          .throw_();
      }
      auto p_plugin = plugins::find<parser_parser_plugin>(p_name->inner);
      if (!p_plugin) {
        throw_parser_not_found(*p_name);
      }
      parser = p_plugin->parse_parser(p);
      TENZIR_DIAG_ASSERT(parser);
    }
    auto ops = std::vector<operator_ptr>{};
    ops.push_back(std::make_unique<load_operator>(std::move(loader)));
    if (decompress) {
      ops.push_back(std::move(decompress));
    }
    ops.push_back(std::make_unique<class read_operator>(std::move(parser)));
    return std::make_unique<pipeline>(std::move(ops));
  }
};

class load_plugin final : virtual public operator_plugin<load_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto usage = "load <loader> <args>...";
    auto docs = "https://docs.tenzir.com/operators/load";
    auto [loader, _] = get_loader(p, usage, docs);
    TENZIR_DIAG_ASSERT(loader);
    return std::make_unique<load_operator>(std::move(loader));
  }
};

class read_plugin final : virtual public operator_plugin<read_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto usage = "read <parser> <args>...";
    auto docs = "https://docs.tenzir.com/operators/read";
    auto name = p.accept_shell_arg();
    if (!name) {
      diagnostic::error("expected parser name")
        .primary(p.current_span())
        .usage(usage)
        .docs(docs)
        .throw_();
    }
    auto plugin = plugins::find<parser_parser_plugin>(name->inner);
    if (!plugin) {
      throw_parser_not_found(*name);
    }
    auto parser = plugin->parse_parser(p);
    TENZIR_DIAG_ASSERT(parser);
    return std::make_unique<class read_operator>(std::move(parser));
  }
};

class from_plugin2 final : public virtual operator_factory_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.from";
  }

  auto make(invocation inv, session ctx) const -> operator_ptr override {
    if (inv.args.empty()) {
      diagnostic::error("expected positional argument `<path/url>`")
        .primary(inv.self)
        .emit(ctx);
      return nullptr;
    }
    auto path_opt = const_eval(inv.args[0], ctx);
    if (not path_opt) {
      return nullptr;
    }
    auto path = caf::get_if<std::string>(&*path_opt);
    if (not path) {
      diagnostic::error("expected string").primary(inv.args[0]).emit(ctx);
      return nullptr;
    }
    // TODO: This is just for demo purposes!
    if (not path->ends_with(".json")) {
      diagnostic::error("`from` currently requires `.json` files")
        .primary(inv.args[0])
        .emit(ctx);
      return nullptr;
    }
    // TODO: Obviously not great.
    auto result = pipeline::internal_parse_as_operator(
      fmt::format("from \"{}\" read json", *path));
    if (not result) {
      diagnostic::error(result.error()).primary(inv.self).emit(ctx);
      return nullptr;
    }
    return std::move(*result);
  }
};

class load_plugin2 final : virtual public operator_factory_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.load";
  }

  auto make(invocation inv, session ctx) const -> operator_ptr override {
    diagnostic::error("operator is not yet implemented")
      .primary(inv.self)
      .emit(ctx);
    return nullptr;
  }
};

} // namespace
} // namespace tenzir::plugins::from

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::from_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::load_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::read_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::from_plugin2)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::load_plugin2)
