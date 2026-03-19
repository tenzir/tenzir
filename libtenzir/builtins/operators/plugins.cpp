//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/context.hpp>
#include <tenzir/data.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::plugins {

namespace {

template <class PluginType, class TypesBuilder>
auto add_plugin_type(const plugin_ptr& plugin, TypesBuilder& types,
                     std::string_view name) -> void {
  if (plugin.as<PluginType>()) {
    types.data(name);
  }
}

auto make_plugins() -> generator<table_slice> {
  auto builder = series_builder{};
  for (const auto& plugin : tenzir::plugins::get()) {
    auto row = builder.record();
    row.field("name").data(plugin->name());
    auto version = std::string{plugin.version() ? plugin.version() : "bundled"};
    row.field("version").data(version);
    row.field("kind").data(fmt::to_string(plugin.type()));
    auto types = row.field("types").list();
    add_plugin_type<aspect_plugin>(plugin, types, "aspect");
    add_plugin_type<command_plugin>(plugin, types, "command");
    add_plugin_type<component_plugin>(plugin, types, "component");
    add_plugin_type<context_plugin>(plugin, types, "context");
    add_plugin_type<loader_parser_plugin>(plugin, types, "loader");
    add_plugin_type<metrics_plugin>(plugin, types, "metrics");
    add_plugin_type<operator_parser_plugin>(plugin, types, "operator");
    add_plugin_type<parser_parser_plugin>(plugin, types, "parser");
    add_plugin_type<printer_parser_plugin>(plugin, types, "printer");
    add_plugin_type<rest_endpoint_plugin>(plugin, types, "rest_endpoint");
    add_plugin_type<saver_parser_plugin>(plugin, types, "saver");
    add_plugin_type<store_plugin>(plugin, types, "store");
    add_plugin_type<aggregation_plugin>(plugin, types,
                                        "tql2.aggregation_function");
    add_plugin_type<function_plugin>(plugin, types, "tql2.function");
    auto dependencies = row.field("dependencies").list();
    for (const auto& dependency : plugin.dependencies()) {
      dependencies.data(dependency);
    }
  }
  for (auto&& slice : builder.finish_as_table_slice("tenzir.plugin")) {
    co_yield std::move(slice);
  }
}

class plugins_operator final : public crtp_operator<plugins_operator> {
public:
  plugins_operator() = default;

  auto operator()() const -> generator<table_slice> {
    for (auto&& slice : make_plugins()) {
      co_yield std::move(slice);
    }
  }

  auto name() const -> std::string override {
    return "plugins";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)order;
    (void)filter;
    return do_not_optimize(*this);
  }

  auto internal() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, plugins_operator& x) -> bool {
    return f.object(x).fields();
  }
};

struct PluginsArgs {
  // No arguments.
};

class Plugins final : public Operator<void, table_slice> {
public:
  explicit Plugins(PluginsArgs /*args*/) {
  }

  auto start(OpCtx&) -> Task<void> override {
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return {};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(result, ctx);
    for (auto&& slice : make_plugins()) {
      co_await push(std::move(slice));
    }
    done_ = true;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("done", done_);
  }

private:
  bool done_ = false;
};

class Plugin final : public virtual operator_plugin<plugins_operator>,
                     public virtual operator_factory_plugin,
                     public virtual OperatorPlugin {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"plugins", "https://docs.tenzir.com/"
                                             "operators/plugins"};
    parser.parse(p);
    return std::make_unique<plugins_operator>();
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    argument_parser2::operator_("plugins").parse(inv, ctx).ignore();
    return std::make_unique<plugins_operator>();
  }

  auto describe() const -> Description override {
    auto d = Describer<PluginsArgs, Plugins>{};
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::plugins

TENZIR_REGISTER_PLUGIN(tenzir::plugins::plugins::Plugin)
