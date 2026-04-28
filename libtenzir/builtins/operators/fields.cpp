//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/async/fetch_node.hpp>
#include <tenzir/async/mail.hpp>
#include <tenzir/catalog.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <caf/actor_registry.hpp>

namespace tenzir::plugins::fields {

namespace {

class fields_operator final : public crtp_operator<fields_operator> {
public:
  fields_operator() = default;

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto catalog
      = ctrl.self().system().registry().get<catalog_actor>("tenzir.catalog");
    TENZIR_ASSERT(catalog);
    ctrl.set_waiting(true);
    auto slices = std::vector<table_slice>{};
    ctrl.self()
      .mail(atom::get_v, std::string{"fields"})
      .request(catalog, caf::infinite)
      .then(
        [&](std::vector<table_slice>& result) {
          slices = std::move(result);
          ctrl.set_waiting(false);
        },
        [&ctrl](const caf::error& err) {
          diagnostic::error(err)
            .note("failed to get fields")
            .emit(ctrl.diagnostics());
        });
    co_yield {};
    for (auto&& slice : slices) {
      co_yield std::move(slice);
    }
  }

  auto name() const -> std::string override {
    return "fields";
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

  friend auto inspect(auto& f, fields_operator& x) -> bool {
    return f.object(x).fields();
  }
};

struct FieldsArgs {
  location operator_location = location::unknown;
};

class Fields final : public Operator<void, table_slice> {
public:
  explicit Fields(FieldsArgs args) : args_{std::move(args)} {
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
    co_return co_await async_mail(atom::get_v, std::string{"fields"})
      .request(catalog_);
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    done_ = true;
    auto& slices_result = result.as<caf::expected<std::vector<table_slice>>>();
    if (not slices_result) {
      diagnostic::error(slices_result.error())
        .primary(args_.operator_location)
        .note("failed to get fields")
        .emit(ctx);
      co_return;
    }
    for (auto&& slice : *slices_result) {
      co_await push(std::move(slice));
    }
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("done", done_);
  }

private:
  FieldsArgs args_;
  catalog_actor catalog_ = {};
  bool done_ = false;
};

class plugin final : public virtual operator_plugin<fields_operator>,
                     public virtual operator_factory_plugin,
                     public virtual OperatorPlugin {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"fields", "https://docs.tenzir.com/"
                                            "operators/fields"};
    parser.parse(p);
    return std::make_unique<fields_operator>();
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    argument_parser2::operator_("fields").parse(inv, ctx).ignore();
    return std::make_unique<fields_operator>();
  }

  auto describe() const -> Description override {
    auto d = Describer<FieldsArgs, Fields>{};
    d.operator_location(&FieldsArgs::operator_location);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::fields

TENZIR_REGISTER_PLUGIN(tenzir::plugins::fields::plugin)
