//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/fetch_node.hpp>
#include <tenzir/async/mail.hpp>
#include <tenzir/catalog.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <caf/actor_registry.hpp>

namespace tenzir::plugins::schemas {

namespace {

struct SchemasArgs {
  location operator_location = location::unknown;
};

class Schemas final : public Operator<void, table_slice> {
public:
  explicit Schemas(SchemasArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto catalog_result = co_await fetch_actor_from_node<catalog_actor>(
      "catalog", args_.operator_location, ctx.actor_system(), ctx);
    if (not catalog_result) {
      done_ = true;
      co_return;
    }
    catalog_ = std::move(*catalog_result);
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    TENZIR_ASSERT(not done_);
    co_return co_await async_mail(atom::get_v, std::string{"schemas"})
      .request(catalog_);
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    done_ = true;
    auto& slices_result = result.as<caf::expected<std::vector<table_slice>>>();
    if (not slices_result) {
      diagnostic::error(slices_result.error())
        .primary(args_.operator_location)
        .note("failed to get schemas")
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
  SchemasArgs args_;
  catalog_actor catalog_ = {};
  bool done_ = false;
};

class SchemasNova final : public Operator<void, nova::Events> {
public:
  explicit SchemasNova(SchemasArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (not node_is_in_process(ctx.actor_system())) {
      diagnostic::error(
        "`schemas` is only available in node pipelines under `--nova`")
        .primary(args_.operator_location)
        .note("event batches cannot cross a process boundary yet")
        .emit(ctx);
      done_ = true;
      co_return;
    }
    auto catalog_result = co_await fetch_actor_from_node<catalog_actor>(
      "catalog", args_.operator_location, ctx.actor_system(), ctx);
    if (not catalog_result) {
      done_ = true;
      co_return;
    }
    catalog_ = std::move(*catalog_result);
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    TENZIR_ASSERT(not done_);
    co_return co_await async_mail(atom::get_v, atom::nova_v,
                                  std::string{"schemas"})
      .request(catalog_);
  }

  auto process_task(Any result, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    done_ = true;
    auto& events_result = result.as<caf::expected<std::vector<nova::Events>>>();
    if (not events_result) {
      diagnostic::error(events_result.error())
        .primary(args_.operator_location)
        .note("failed to get schemas")
        .emit(ctx);
      co_return;
    }
    for (auto&& events : *events_result) {
      co_await push(std::move(events));
    }
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("done", done_);
  }

private:
  SchemasArgs args_;
  catalog_actor catalog_ = {};
  bool done_ = false;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "schemas";
  }

  auto describe() const -> Description override {
    auto d = Describer<SchemasArgs, Schemas, SchemasNova>{};
    d.operator_location(&SchemasArgs::operator_location);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::schemas

TENZIR_REGISTER_PLUGIN(tenzir::plugins::schemas::plugin)
