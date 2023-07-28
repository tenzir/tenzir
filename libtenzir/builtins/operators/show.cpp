//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

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

auto table_type() -> type {
  return type{
    "tenzir.table",
    record_type{
      {"uuid", string_type{}},
      {"memory_usage", uint64_type{}},
      {"min_import_time", time_type{}},
      {"max_import_time", time_type{}},
      {"version", uint64_type{}},
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
    if (args_.aspect.inner == "tables") {
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
      auto builder = table_slice_builder{table_type()};
      // FIXME: ensure that we do no use more most 2^15 rows.
      for (const auto& synopsis : synopses) {
        // TODO: add schema
        if (not(builder.add(fmt::to_string(synopsis.uuid))
                && builder.add(synopsis.synopsis->memusage())
                && builder.add(synopsis.synopsis->min_import_time)
                && builder.add(synopsis.synopsis->max_import_time)
                && builder.add(synopsis.synopsis->version))) {
          diagnostic::error("failed to add table entry")
            .note("from `show`")
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

  auto location() const -> operator_location override {
    return operator_location::remote;
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
    auto aspects = std::set<std::string_view>{"tables"};
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
