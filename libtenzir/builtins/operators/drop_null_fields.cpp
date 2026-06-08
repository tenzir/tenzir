//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/drop_null_fields.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/type.hpp>

namespace tenzir::plugins::drop_null_fields {

namespace {

struct DropNullFieldsArgs {
  std::vector<ast::expression> fields;
  event_order order = event_order::ordered;
};

class DropNullFields final : public Operator<table_slice, table_slice> {
public:
  explicit DropNullFields(DropNullFieldsArgs args) : order_{args.order} {
    if (args.fields.size() == 1) {
      auto selector = ast::field_path::try_from(args.fields.front());
      TENZIR_ASSERT(selector);
      if (selector->has_this() and selector->path().empty()) {
        return;
      }
    }
    selectors_.reserve(args.fields.size());
    for (auto& arg : args.fields) {
      auto selector = ast::field_path::try_from(arg);
      TENZIR_ASSERT(selector);
      TENZIR_ASSERT(not selector->has_this());
      selectors_.push_back(std::move(*selector));
    }
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto output = tenzir::drop_null_fields(std::move(input), selectors_, order_,
                                           ctx.dh());
    for (auto& slice : output) {
      co_await push(std::move(slice));
    }
  }

private:
  std::vector<ast::field_path> selectors_;
  event_order order_ = event_order::ordered;
};

class drop_null_fields_operator final
  : public crtp_operator<drop_null_fields_operator> {
public:
  drop_null_fields_operator() = default;

  explicit drop_null_fields_operator(std::vector<ast::field_path> selectors,
                                     event_order order = event_order::ordered)
    : selectors_{std::move(selectors)}, order_{order} {
  }

  auto name() const -> std::string override {
    return "tql2.drop_null_fields";
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    for (auto&& slice : input) {
      auto output = tenzir::drop_null_fields(std::move(slice), selectors_,
                                             order_, ctrl.diagnostics());
      for (auto& part : output) {
        co_yield std::move(part);
      }
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter);
    return optimize_result{
      std::nullopt, order,
      std::make_unique<drop_null_fields_operator>(selectors_, order)};
  }

  friend auto inspect(auto& f, drop_null_fields_operator& x) -> bool {
    return f.object(x)
      .pretty_name("drop_null_fields_operator")
      .fields(f.field("selectors", x.selectors_), f.field("order", x.order_));
  }

private:
  std::vector<ast::field_path> selectors_;
  event_order order_ = event_order::ordered;
};

} // namespace

class plugin final : public virtual operator_plugin2<drop_null_fields_operator>,
                     public virtual OperatorPlugin {
public:
  auto describe() const -> Description override {
    auto d = Describer<DropNullFieldsArgs, DropNullFields>{};
    auto fields
      = d.optional_variadic("fields", &DropNullFieldsArgs::fields, "field");
    d.optimization_order(&DropNullFieldsArgs::order);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      auto values = ctx.get_all(fields);
      auto locations = ctx.get_locations(fields);
      TENZIR_ASSERT(values.size() == locations.size());
      if (values.size() == 1 and values[0]) {
        auto selector = ast::field_path::try_from(*values[0]);
        if (selector and selector->has_this() and selector->path().empty()) {
          return {};
        }
      }
      for (auto i = 0uz; i < values.size(); ++i) {
        if (not values[i]) {
          diagnostic::error("expected simple selector")
            .primary(locations[i])
            .emit(ctx);
          continue;
        }
        auto selector = ast::field_path::try_from(*values[i]);
        if (not selector) {
          diagnostic::error("expected simple selector")
            .primary(locations[i])
            .emit(ctx);
          continue;
        }
        if (selector->has_this()) {
          diagnostic::error("cannot drop `this`")
            .primary(locations[i])
            .emit(ctx);
        }
      }
      return {};
    });
    return d.invariant_order();
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_("drop_null_fields");
    auto selectors = std::vector<ast::field_path>{};
    // Special case: allow "drop_null_fields this" to behave like no arguments
    if (inv.args.size() == 1) {
      auto selector = ast::field_path::try_from(inv.args[0]);
      if (selector and selector->has_this() and selector->path().empty()) {
        // "this" with no path - treat as no arguments
        return std::make_unique<drop_null_fields_operator>(
          std::move(selectors));
      }
    }
    for (auto& arg : inv.args) {
      auto selector = ast::field_path::try_from(arg);
      if (selector) {
        if (selector->has_this()) {
          diagnostic::error("cannot drop `this`").primary(*selector).emit(ctx);
          return failure::promise();
        }
        selectors.push_back(std::move(*selector));
      } else {
        diagnostic::error("expected simple selector")
          .primary(arg)
          .usage(parser.usage())
          .docs(parser.docs())
          .emit(ctx.dh());
        return failure::promise();
      }
    }
    return std::make_unique<drop_null_fields_operator>(std::move(selectors));
  }
};

} // namespace tenzir::plugins::drop_null_fields

TENZIR_REGISTER_PLUGIN(tenzir::plugins::drop_null_fields::plugin)
