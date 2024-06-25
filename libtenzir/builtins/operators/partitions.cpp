//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/actors.hpp>
#include <tenzir/argument_parser.hpp>
#include <tenzir/catalog.hpp>
#include <tenzir/node_control.hpp>
#include <tenzir/partition_synopsis.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/si_literals.hpp>

#include <caf/scoped_actor.hpp>

namespace tenzir::plugins::partitions {

namespace {

class partitions_operator final : public crtp_operator<partitions_operator> {
public:
  partitions_operator() = default;
  explicit partitions_operator(expression filter) : filter_{std::move(filter)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    // TODO: Some of the the requests this operator makes are blocking, so
    // we have to create a scoped actor here; once the operator API uses
    // async we can offer a better mechanism here.
    auto blocking_self = caf::scoped_actor(ctrl.self().system());
    auto components
      = get_node_components<catalog_actor>(blocking_self, ctrl.node());
    if (!components) {
      diagnostic::error(components.error())
        .note("failed to get catalog")
        .emit(ctrl.diagnostics());
      co_return;
    }
    co_yield {};
    auto [catalog] = std::move(*components);
    auto synopses = std::vector<partition_synopsis_pair>{};
    ctrl.set_waiting(true);
    ctrl.self()
      .request(catalog, caf::infinite, atom::get_v, filter_)
      .await(
        [&](std::vector<partition_synopsis_pair>& result) {
          ctrl.set_waiting(false);
          synopses = std::move(result);
        },
        [&](const caf::error& err) {
          diagnostic::error(err)
            .note("failed to perform catalog lookup")
            .emit(ctrl.diagnostics());
        });
    co_yield {};
    auto builder = series_builder{};
    using namespace tenzir::si_literals;
    constexpr auto max_rows = size_t{8_Ki};
    for (auto i = 0u; i < synopses.size(); ++i) {
      auto& synopsis = synopses[i];
      auto event = builder.record();
      event.field("uuid").data(fmt::to_string(synopsis.uuid));
      event.field("memusage").data(synopsis.synopsis->memusage());
      event.field("diskusage")
        .data(synopsis.synopsis->store_file.size
              + synopsis.synopsis->indexes_file.size
              + synopsis.synopsis->sketches_file.size);
      event.field("events").data(synopsis.synopsis->events);
      event.field("min_import_time").data(synopsis.synopsis->min_import_time);
      event.field("max_import_time").data(synopsis.synopsis->max_import_time);
      event.field("version").data(synopsis.synopsis->version);
      event.field("schema").data(synopsis.synopsis->schema.name());
      event.field("schema_id")
        .data(synopsis.synopsis->schema.make_fingerprint());
      event.field("internal")
        .data(synopsis.synopsis->schema.attribute("internal").has_value());
      auto add_resource = [&](std::string_view key, const resource& value) {
        auto x = event.field(key).record();
        x.field("url").data(value.url);
        x.field("size").data(value.size);
      };
      add_resource("store", synopsis.synopsis->store_file);
      add_resource("indexes", synopsis.synopsis->indexes_file);
      add_resource("sketches", synopsis.synopsis->sketches_file);
      if ((i + 1) % max_rows == 0) {
        for (auto&& result :
             builder.finish_as_table_slice("tenzir.partition")) {
          co_yield std::move(result);
        }
      }
    }
    for (auto&& result : builder.finish_as_table_slice("tenzir.partition")) {
      co_yield std::move(result);
    }
  }

  auto name() const -> std::string override {
    return "partitions";
  }

  auto location() const -> operator_location override {
    return operator_location::remote;
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

  friend auto inspect(auto& f, partitions_operator& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.partitions.partitions_operator")
      .fields(f.field("filter", x.filter_));
  }

private:
  expression filter_ = trivially_true_expression();
};

class plugin final : public virtual operator_plugin<partitions_operator> {
public:
  auto signature() const -> operator_signature override {
    return {
      .source = true,
      .transformation = false,
      .sink = false,
    };
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{
      "partitions",
      "https://docs.tenzir.com/operators/partitions",
    };
    auto expr = std::optional<located<tenzir::expression>>{};
    parser.add(expr, "<expr>");
    parser.parse(p);
    if (not expr) {
      return std::make_unique<partitions_operator>();
    }
    auto normalized_and_validated = normalize_and_validate(expr->inner);
    if (!normalized_and_validated) {
      diagnostic::error("invalid expression")
        .primary(expr->source)
        .docs("https://tenzir.com/language/expressions")
        .throw_();
    }
    expr->inner = std::move(*normalized_and_validated);
    return std::make_unique<partitions_operator>(std::move(expr->inner));
  }
};

} // namespace

} // namespace tenzir::plugins::partitions

TENZIR_REGISTER_PLUGIN(tenzir::plugins::partitions::plugin)
