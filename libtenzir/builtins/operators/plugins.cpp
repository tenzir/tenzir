//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/data.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::plugins {

namespace {

class plugins_operator final : public crtp_operator<plugins_operator> {
public:
  plugins_operator() = default;

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto builder = series_builder{};
    for (const auto& plugin : tenzir::plugins::get()) {
      auto row = builder.record();
      row.field("name").data(plugin->name());
      auto version
        = std::string{plugin.version() ? plugin.version() : "bundled"};
      row.field("version").data(version);
      row.field("kind").data(fmt::to_string(plugin.type()));
      auto types = row.field("types").list();
#define TENZIR_ADD_PLUGIN_TYPE(type, name)                                     \
  do {                                                                         \
    if (plugin.as<type##_plugin>()) {                                          \
      types.data(name);                                                        \
    }                                                                          \
  } while (false)
      TENZIR_ADD_PLUGIN_TYPE(aggregation_function, "aggregation_function");
      TENZIR_ADD_PLUGIN_TYPE(aspect, "aspect");
      TENZIR_ADD_PLUGIN_TYPE(command, "command");
      TENZIR_ADD_PLUGIN_TYPE(component, "component");
      TENZIR_ADD_PLUGIN_TYPE(context, "context");
      TENZIR_ADD_PLUGIN_TYPE(loader_parser, "loader");
      TENZIR_ADD_PLUGIN_TYPE(metrics, "metrics");
      TENZIR_ADD_PLUGIN_TYPE(operator_parser, "operator");
      TENZIR_ADD_PLUGIN_TYPE(parser_parser, "parser");
      TENZIR_ADD_PLUGIN_TYPE(printer_parser, "printer");
      TENZIR_ADD_PLUGIN_TYPE(rest_endpoint, "rest_endpoint");
      TENZIR_ADD_PLUGIN_TYPE(saver_parser, "saver");
      TENZIR_ADD_PLUGIN_TYPE(store, "store");
      TENZIR_ADD_PLUGIN_TYPE(operator_factory, "tql2.operator");
      TENZIR_ADD_PLUGIN_TYPE(aggregation, "tql2.aggregation_function");
      TENZIR_ADD_PLUGIN_TYPE(function, "tql2.function");
#undef TENZIR_ADD_PLUGIN_TYPE
      auto dependencies = row.field("dependencies").list();
      for (const auto& dependency : plugin.dependencies()) {
        dependencies.data(dependency);
      }
    }
    for (auto&& slice : builder.finish_as_table_slice("tenzir.plugin")) {
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

class plugin final : public virtual operator_plugin<plugins_operator>,
                     operator_factory_plugin {
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

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    argument_parser2::operator_("plugins").parse(inv, ctx).ignore();
    return std::make_unique<plugins_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::plugins

TENZIR_REGISTER_PLUGIN(tenzir::plugins::plugins::plugin)
