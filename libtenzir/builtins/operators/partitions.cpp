//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/actors.hpp>
#include <tenzir/async/fetch_node.hpp>
#include <tenzir/async/mail.hpp>
#include <tenzir/catalog.hpp>
#include <tenzir/double_synopsis.hpp>
#include <tenzir/duration_synopsis.hpp>
#include <tenzir/int64_synopsis.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/partition_synopsis.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/time_synopsis.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/uint64_synopsis.hpp>

#include <caf/actor_registry.hpp>
#include <caf/scoped_actor.hpp>

#include <string_view>

namespace tenzir::plugins::partitions {

namespace {

struct PartitionsArgs {
  Option<ast::expression> predicate;
  location operator_location = location::unknown;
};

class Partitions final : public Operator<void, table_slice> {
public:
  explicit Partitions(PartitionsArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto catalog_result = co_await fetch_actor_from_node<catalog_actor>(
      "catalog", args_.operator_location, ctx.actor_system(), ctx);
    if (not catalog_result) {
      done_ = true;
      co_return;
    }
    catalog_ = std::move(*catalog_result);
    if (args_.predicate) {
      auto [legacy, _] = split_legacy_expression(*args_.predicate);
      filter_ = std::move(legacy);
    }
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    TENZIR_ASSERT(not done_);
    co_return co_await async_mail(atom::get_v, std::string{"partitions"},
                                  filter_)
      .request(catalog_);
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    done_ = true;
    auto& slices_result = result.as<caf::expected<std::vector<table_slice>>>();
    if (not slices_result) {
      diagnostic::error(slices_result.error())
        .primary(args_.operator_location)
        .note("failed to perform catalog lookup")
        .emit(ctx);
      co_return;
    }
    for (auto&& slice : *slices_result) {
      co_await push(std::move(slice));
    }
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("done", done_);
  }

private:
  PartitionsArgs args_;
  catalog_actor catalog_ = {};
  expression filter_ = trivially_true_expression();
  bool done_ = false;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "partitions";
  }

  auto describe() const -> Description override {
    auto d = Describer<PartitionsArgs, Partitions>{};
    auto predicate
      = d.positional("predicate", &PartitionsArgs::predicate, "bool");
    d.operator_location(&PartitionsArgs::operator_location);
    d.validate([predicate](DescribeCtx& ctx) -> Empty {
      if (auto expr = ctx.get(predicate)) {
        auto [legacy, remainder] = split_legacy_expression(*expr);
        TENZIR_UNUSED(legacy);
        if (not is_true_literal(remainder)) {
          diagnostic::warning("`partitions` only supports the legacy subset "
                              "of this predicate")
            .note("the used predicate will be wider than the given one")
            .primary(expr->get_location())
            .emit(ctx);
        }
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::partitions

TENZIR_REGISTER_PLUGIN(tenzir::plugins::partitions::plugin)
