//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/argument_parser.hpp>
#include <vast/diagnostics.hpp>
#include <vast/parser_interface.hpp>
#include <vast/plugin.hpp>
#include <vast/tql/fwd.hpp>
#include <vast/tql/parser.hpp>

namespace vast::plugins::from {
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
    return caf::make_error(ec::unspecified, "could not instantiate loader");
  }

  auto detached() const -> bool override {
    return true;
  }

  auto name() const -> std::string override {
    return "load";
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
                                       to_string(), operator_type_name(input)));
  }

private:
  std::unique_ptr<plugin_loader> loader_;
};

class parse_operator final : public crtp_operator<parse_operator> {
public:
  parse_operator() = default;

  explicit parse_operator(std::unique_ptr<plugin_parser> parser)
    : parser_{std::move(parser)} {
  }

  auto name() const -> std::string override {
    return "parse";
  }

  friend auto inspect(auto& f, parse_operator& x) -> bool {
    return plugin_inspect(f, x.parser_);
  }

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> caf::expected<generator<table_slice>> {
    auto parser = parser_->instantiate(std::move(input), ctrl);
    if (not parser) {
      return caf::make_error(ec::unspecified, "could not instantiate parser");
    }
    return std::move(*parser);
  }

private:
  std::unique_ptr<plugin_parser> parser_;
};

/// @throws `diagnostic`
auto parse_default_parser(std::string definition)
  -> std::unique_ptr<plugin_parser> {
  // We discard all diagnostics emitted for the default parser because the
  // source has not been written by the user.
  auto diag = null_diagnostic_handler{};
  auto p = tql::make_parser_interface(std::move(definition), diag);
  auto p_name = p->accept_identifier();
  VAST_DIAG_ASSERT(p_name);
  auto const* p_plugin = plugins::find<parser_parser_plugin>(p_name->name);
  VAST_DIAG_ASSERT(p_plugin);
  auto parser = p_plugin->parse_parser(*p);
  VAST_DIAG_ASSERT(parser);
  return parser;
}

[[noreturn]] void throw_loader_not_found(const located<std::string>& x) {
  auto available = std::vector<std::string>{};
  for (auto const* p : plugins::get<loader_parser_plugin>()) {
    available.push_back(p->name());
  }
  diagnostic::error("loader `{}` could not be found", x.inner)
    .primary(x.source)
    .hint("must be one of {}", fmt::join(available, ", "))
    .docs("https://vast.io/docs/next/connectors")
    .throw_();
}

[[noreturn]] void throw_parser_not_found(const located<std::string>& x) {
  auto available = std::vector<std::string>{};
  for (auto p : plugins::get<parser_parser_plugin>()) {
    available.push_back(p->name());
  }
  diagnostic::error("parser `{}` could not be found", x.inner)
    .primary(x.source)
    .hint("must be one of {}", fmt::join(available, ", "))
    .docs("https://vast.io/docs/next/formats")
    .throw_();
}

class from_plugin final : public virtual operator_parser_plugin {
public:
  auto name() const -> std::string override {
    return "from";
  };

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto usage = "from <loader> <args>... [read <parser> <args>...]";
    auto docs = "https://vast.io/docs/next/operators/sources/from";
    auto l_name = p.accept_shell_arg();
    if (!l_name) {
      diagnostic::error("expected loader name")
        .primary(p.current_span())
        .usage(usage)
        .docs(docs)
        .throw_();
    }
    auto l_plugin = plugins::find<loader_parser_plugin>(l_name->inner);
    if (!l_plugin) {
      throw_loader_not_found(*l_name);
    }
    auto q = until_keyword_parser{"read", p};
    auto loader = l_plugin->parse_loader(q);
    VAST_DIAG_ASSERT(loader);
    VAST_DIAG_ASSERT(q.at_end());
    auto parser = std::unique_ptr<plugin_parser>{};
    if (p.at_end()) {
      parser = parse_default_parser(loader->default_parser());
    } else {
      auto read = p.accept_identifier();
      VAST_DIAG_ASSERT(read && read->name == "read");
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
      VAST_DIAG_ASSERT(parser);
    }
    auto ops = std::vector<operator_ptr>{};
    ops.push_back(std::make_unique<load_operator>(std::move(loader)));
    ops.push_back(std::make_unique<class parse_operator>(std::move(parser)));
    return std::make_unique<pipeline>(std::move(ops));
  }
};

auto make_stdin_loader() -> std::unique_ptr<plugin_loader> {
  auto diag = null_diagnostic_handler{};
  auto plugin = plugins::find<loader_parser_plugin>("file");
  VAST_DIAG_ASSERT(plugin);
  auto parser = tql::make_parser_interface("-", diag);
  auto loader = plugin->parse_loader(*parser);
  VAST_DIAG_ASSERT(loader);
  return loader;
}

class read_plugin final : public virtual operator_parser_plugin {
public:
  auto name() const -> std::string override {
    return "read";
  };

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto usage = "read <parser> <args>... [from <loader> <args>...]";
    auto docs = "https://vast.io/docs/next/operators/sources/read";
    auto p_name = p.accept_shell_arg();
    if (!p_name) {
      diagnostic::error("expected parser name")
        .primary(p.current_span())
        .usage(usage)
        .docs(docs)
        .throw_();
    }
    auto p_plugin = plugins::find<parser_parser_plugin>(p_name->inner);
    if (!p_plugin) {
      throw_parser_not_found(*p_name);
    }
    auto q = until_keyword_parser{"from", p};
    auto parser = p_plugin->parse_parser(q);
    VAST_DIAG_ASSERT(parser);
    VAST_DIAG_ASSERT(q.at_end());
    auto loader = std::unique_ptr<plugin_loader>{};
    if (p.at_end()) {
      loader = make_stdin_loader();
    } else {
      auto from = p.accept_identifier();
      VAST_DIAG_ASSERT(from && from->name == "from");
      auto l_name = p.accept_shell_arg();
      if (!l_name) {
        diagnostic::error("expected loader name")
          .primary(p.current_span())
          .note(usage)
          .docs(docs)
          .throw_();
      }
      auto l_plugin = plugins::find<loader_parser_plugin>(l_name->inner);
      if (!l_plugin) {
        throw_loader_not_found(*l_name);
      }
      loader = l_plugin->parse_loader(p);
      VAST_DIAG_ASSERT(parser);
    }
    auto ops = std::vector<operator_ptr>{};
    ops.push_back(std::make_unique<load_operator>(std::move(loader)));
    ops.push_back(std::make_unique<class parse_operator>(std::move(parser)));
    return std::make_unique<pipeline>(std::move(ops));
  }
};

class load_plugin final : virtual public operator_plugin<load_operator> {
public:
  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto usage = "load <loader> <args>...";
    auto docs = "https://vast.io/docs/next/operators/sources/load";
    auto name = p.accept_shell_arg();
    if (!name) {
      diagnostic::error("expected loader name", p.current_span())
        .primary(p.current_span())
        .usage(usage)
        .docs(docs)
        .throw_();
    }
    auto plugin = plugins::find<loader_parser_plugin>(name->inner);
    if (!plugin) {
      throw_loader_not_found(*name);
    }
    auto loader = plugin->parse_loader(p);
    VAST_DIAG_ASSERT(loader);
    return std::make_unique<load_operator>(std::move(loader));
  }
};

class parse_plugin final : virtual public operator_plugin<parse_operator> {
public:
  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto usage = "parse <parser> <args>...";
    auto docs = "https://vast.io/docs/next/operators/transformations/parse";
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
    VAST_DIAG_ASSERT(parser);
    return std::make_unique<class parse_operator>(std::move(parser));
  }
};

} // namespace
} // namespace vast::plugins::from

VAST_REGISTER_PLUGIN(vast::plugins::from::from_plugin)
VAST_REGISTER_PLUGIN(vast::plugins::from::read_plugin)
VAST_REGISTER_PLUGIN(vast::plugins::from::load_plugin)
VAST_REGISTER_PLUGIN(vast::plugins::from::parse_plugin)
