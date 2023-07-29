//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/collect.hpp"

#include <tenzir/actors.hpp>
#include <tenzir/argument_parser.hpp>
#include <tenzir/atoms.hpp>
#include <tenzir/catalog.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/node_control.hpp>
#include <tenzir/partition_synopsis.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/uuid.hpp>

#include <caf/scoped_actor.hpp>
#include <caf/timespan.hpp>

#include <string>
#include <vector>

namespace tenzir::plugins::show {

auto connector_type() -> type {
  return type{
    "tenzir.connector",
    record_type{
      {"name", string_type{}},
      {"loader", bool_type{}},
      {"saver", bool_type{}},
    },
  };
}

auto format_type() -> type {
  return type{
    "tenzir.format",
    record_type{
      {"name", string_type{}},
      {"printer", bool_type{}},
      {"parser", bool_type{}},
    },
  };
}

auto operator_type() -> type {
  return type{
    "tenzir.operator",
    record_type{
      {"name", string_type{}},
      {"source", bool_type{}},
      {"transformation", bool_type{}},
      {"sink", bool_type{}},
    },
  };
}

auto partition_type() -> type {
  return type{
    "tenzir.partition",
    record_type{
      {"uuid", string_type{}},
      {"memory_usage", uint64_type{}},
      {"min_import_time", time_type{}},
      {"max_import_time", time_type{}},
      {"version", uint64_type{}},
      {"schema", string_type{}},
    },
  };
}

struct operator_args {
  located<std::string> aspect;

  friend auto inspect(auto& f, operator_args& x) -> bool {
    return f.object(x)
      .pretty_name("operator_args")
      .fields(f.field("aspect", x.aspect));
  }
};

class show_operator final : public crtp_operator<show_operator> {
public:
  show_operator() = default;

  explicit show_operator(operator_args args) : args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto contains = [](const auto& plugins, const auto& name) -> bool {
      auto f = [&](const auto& plugin) {
        return plugin->name() == name;
      };
      return std::find_if(plugins.begin(), plugins.end(), f) != plugins.end();
    };
    if (args_.aspect.inner == "connectors") {
      auto loaders = collect(plugins::get<loader_parser_plugin>());
      auto savers = collect(plugins::get<saver_parser_plugin>());
      auto connectors = std::set<std::string>{};
      for (const auto* plugin : loaders)
        connectors.insert(plugin->name());
      for (const auto* plugin : savers)
        connectors.insert(plugin->name());
      auto builder = table_slice_builder{connector_type()};
      for (const auto& connector : connectors) {
        if (not(builder.add(connector)
                && builder.add(contains(loaders, connector))
                && builder.add(contains(savers, connector)))) {
          diagnostic::error("failed to add connector")
            .note("from `show {}`", args_.aspect.inner)
            .emit(ctrl.diagnostics());
          co_return;
        }
      }
      co_yield builder.finish();
    } else if (args_.aspect.inner == "formats") {
      auto parsers = collect(plugins::get<parser_parser_plugin>());
      auto printers = collect(plugins::get<printer_parser_plugin>());
      auto formats = std::set<std::string>{};
      for (const auto* plugin : parsers)
        formats.insert(plugin->name());
      for (const auto* plugin : printers)
        formats.insert(plugin->name());
      auto builder = table_slice_builder{format_type()};
      for (const auto& format : formats) {
        if (not(builder.add(format) && builder.add(contains(parsers, format))
                && builder.add(contains(printers, format)))) {
          diagnostic::error("failed to add format")
            .note("from `show {}`", args_.aspect.inner)
            .emit(ctrl.diagnostics());
          co_return;
        }
      }
      co_yield builder.finish();
    } else if (args_.aspect.inner == "operators") {
      auto builder = table_slice_builder{operator_type()};
      for (const auto* plugin : plugins::get<operator_parser_plugin>()) {
        // TODO: figure out how we can get the operator type. Ideally also it's
        // arguments.
        if (not(builder.add(plugin->name()) && builder.add(false)
                && builder.add(false) && builder.add(false))) {
          diagnostic::error("failed to add operator")
            .note("from `show {}`", args_.aspect.inner)
            .emit(ctrl.diagnostics());
          co_return;
        }
      }
      co_yield builder.finish();
    } else if (args_.aspect.inner == "partitions") {
      // TODO: Some of the the requests this operator makes are blocking, so we
      // have to create a scoped actor here; once the operator API uses async we
      // can offer a better mechanism here.
      auto blocking_self = caf::scoped_actor(ctrl.self().system());
      auto components
        = get_node_components<catalog_actor>(blocking_self, ctrl.node());
      if (!components) {
        ctrl.abort(std::move(components.error()));
        co_return;
      }
      co_yield {};
      auto [catalog] = std::move(*components);

      auto synopses = std::vector<partition_synopsis_pair>{};
      auto error = caf::error{};
      ctrl.self()
        .request(catalog, caf::infinite, atom::get_v)
        .await(
          [&synopses](std::vector<partition_synopsis_pair> result) {
            synopses = std::move(result);
          },
          [&error](caf::error err) {
            error = std::move(err);
          });
      co_yield {};
      if (error) {
        ctrl.abort(std::move(error));
        co_return;
      }
      auto builder = table_slice_builder{partition_type()};
      // FIXME: ensure that we do no use more most 2^15 rows.
      for (const auto& synopsis : synopses) {
        // TODO: add complete schema
        if (not(builder.add(fmt::to_string(synopsis.uuid))
                && builder.add(uint64_t{synopsis.synopsis->memusage()})
                && builder.add(synopsis.synopsis->min_import_time)
                && builder.add(synopsis.synopsis->max_import_time)
                && builder.add(synopsis.synopsis->version)
                && builder.add(synopsis.synopsis->schema.name()))) {
          diagnostic::error("failed to add partition entry")
            .note("from `show {}`", args_.aspect.inner)
            .emit(ctrl.diagnostics());
          co_return;
        }
      }
      co_yield builder.finish();
    } else {
      die("unchecked aspect");
    }
  }

  auto name() const -> std::string override {
    return "show";
  }

  auto detached() const -> bool override {
    return true;
  }

  // TODO: is it a good idea to make this function conditional?
  auto location() const -> operator_location override {
    if (args_.aspect.inner == "partitions")
      return operator_location::remote;
    return operator_location::local;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)order;
    (void)filter;
    return optimize_result{std::nullopt, event_order::unordered, copy()};
  }

  friend auto inspect(auto& f, show_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  operator_args args_;
};

class plugin final : public virtual operator_plugin<show_operator> {
public:
  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"show", "https://docs.tenzir.com/next/"
                                          "operators/sources/show"};
    operator_args args;
    parser.add(args.aspect, "<aspect>");
    parser.parse(p);
    auto aspects = std::set<std::string_view>{
      "connectors",
      "formats",
      "operators",
      "partitions",
    };
    if (not aspects.contains(args.aspect.inner))
      diagnostic::error("aspect `{}` could not be found", args.aspect.inner)
        .primary(args.aspect.source)
        .hint("must be one of {}", fmt::join(aspects, ", "))
        .throw_();
    return std::make_unique<show_operator>(std::move(args));
  }
};

} // namespace tenzir::plugins::show

TENZIR_REGISTER_PLUGIN(tenzir::plugins::show::plugin)
