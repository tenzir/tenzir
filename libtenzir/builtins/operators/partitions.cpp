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
#include <tenzir/double_synopsis.hpp>
#include <tenzir/duration_synopsis.hpp>
#include <tenzir/int64_synopsis.hpp>
#include <tenzir/node_control.hpp>
#include <tenzir/partition_synopsis.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/time_synopsis.hpp>
#include <tenzir/uint64_synopsis.hpp>

#include <caf/scoped_actor.hpp>

namespace tenzir::plugins::partitions {

namespace {

class partitions_operator final : public crtp_operator<partitions_operator> {
public:
  partitions_operator() = default;
  explicit partitions_operator(expression filter,
                               bool experimental_include_ranges)
    : filter_{std::move(filter)},
      experimental_include_ranges_{experimental_include_ranges} {
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
    auto builders = std::unordered_map<type, series_builder>{};
    using namespace tenzir::si_literals;
    for (auto& synopsis : synopses) {
      auto& builder
        = builders[experimental_include_ranges_ ? synopsis.synopsis->schema
                                                : type{}];
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
      if (experimental_include_ranges_) {
        auto ranges = event.field("ranges").record();
#define X(name)                                                                \
  do {                                                                         \
    auto entry = ranges.field(#name).record();                                 \
    const auto it                                                              \
      = synopsis.synopsis->type_synopses_.find(type{name##_type{}});           \
    if (it == synopsis.synopsis->type_synopses_.end()) {                       \
      entry.field("min").null();                                               \
      entry.field("max").null();                                               \
    } else {                                                                   \
      const auto* syn                                                          \
        = dynamic_cast<const name##_synopsis*>(it->second.get());              \
      TENZIR_ASSERT(syn);                                                      \
      entry.field("min").data(syn->min());                                     \
      entry.field("max").data(syn->max());                                     \
    }                                                                          \
  } while (0)
        X(time);
        X(duration);
        X(uint64);
        X(int64);
        X(double);
#undef X
        auto fields = ranges.field("fields").record();
        for (const auto& [qf, synopsis] : synopsis.synopsis->field_synopses_) {
#define X(name)                                                                \
  if (const auto* syn                                                          \
      = dynamic_cast<const name##_synopsis*>(synopsis.get())) {                \
    auto entry = fields.field(qf.field_name()).record();                       \
    entry.field("min").data(syn->min());                                       \
    entry.field("max").data(syn->max());                                       \
    continue;                                                                  \
  }
          X(time);
          X(duration);
          X(uint64);
          X(int64);
          X(double);
#undef X
        }
      }
    }
    for (auto& [_, builder] : builders) {
      for (auto&& result : builder.finish_as_table_slice("tenzir.partition")) {
        co_yield std::move(result);
      }
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
      .fields(f.field("filter", x.filter_),
              f.field("experimental_include_ranges",
                      x.experimental_include_ranges_));
  }

private:
  expression filter_ = trivially_true_expression();
  bool experimental_include_ranges_ = {};
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
    auto experimental_include_ranges = std::optional<location>{};
    parser.add(expr, "<expr>");
    // This option is a temporary workaround to allow for inspecting the min and
    // max time values of partitions in setups where these values differ from
    // the import timestamps. A proper solution for this should be implemented
    // as a standalone operator that takes a field or type extractor and returns
    // indexes relevant for it.
    parser.add("--experimental-include-ranges", experimental_include_ranges);
    parser.parse(p);
    if (not expr) {
      return std::make_unique<partitions_operator>(
        trivially_true_expression(), experimental_include_ranges.has_value());
    }
    auto normalized_and_validated = normalize_and_validate(expr->inner);
    if (!normalized_and_validated) {
      diagnostic::error("invalid expression")
        .primary(expr->source)
        .docs("https://tenzir.com/language/expressions")
        .throw_();
    }
    expr->inner = std::move(*normalized_and_validated);
    return std::make_unique<partitions_operator>(
      std::move(expr->inner), experimental_include_ranges.has_value());
  }
};

} // namespace

} // namespace tenzir::plugins::partitions

TENZIR_REGISTER_PLUGIN(tenzir::plugins::partitions::plugin)
