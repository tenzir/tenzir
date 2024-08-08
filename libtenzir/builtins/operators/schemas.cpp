//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/catalog.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::schemas {

namespace {

class schemas_operator final : public crtp_operator<schemas_operator> {
public:
  schemas_operator() = default;

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto catalog
      = ctrl.self().system().registry().get<catalog_actor>("tenzir.catalog");
    TENZIR_ASSERT(catalog);
    ctrl.set_waiting(true);
    auto schemas = std::unordered_set<type>{};
    ctrl.self()
      .request(catalog, caf::infinite, atom::get_v)
      .then(
        [&](std::vector<partition_synopsis_pair>& synopses) {
          for (const auto& [id, synopsis] : synopses) {
            TENZIR_ASSERT(synopsis);
            TENZIR_ASSERT(synopsis->schema);
            schemas.insert(synopsis->schema);
          }
          ctrl.set_waiting(false);
        },
        [&ctrl](const caf::error& err) {
          diagnostic::error(err)
            .note("failed to get partitions")
            .emit(ctrl.diagnostics());
        });
    co_yield {};
    auto builder = series_builder{};
    for (const auto& schema : schemas) {
      builder.data(schema.to_definition());
      co_yield builder.finish_assert_one_slice(
        fmt::format("tenzir.schema.{}", schema.make_fingerprint()));
    }
  }

  auto name() const -> std::string override {
    return "schemas";
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

  friend auto inspect(auto& f, schemas_operator& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual operator_plugin<schemas_operator>,
                     operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"schemas", "https://docs.tenzir.com/"
                                             "operators/schemas"};
    parser.parse(p);
    return std::make_unique<schemas_operator>();
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    argument_parser2::operator_("schemas").parse(inv, ctx).ignore();
    return std::make_unique<schemas_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::schemas

TENZIR_REGISTER_PLUGIN(tenzir::plugins::schemas::plugin)
