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
  OptimizationArgs<opt::Order> optimization;
};

class DropNullFields final : public Operator<table_slice, table_slice> {
public:
  explicit DropNullFields(DropNullFieldsArgs args)
    : order_{args.optimization.order} {
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
  EventOrder order_ = EventOrder::ordered;
};

} // namespace

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "drop_null_fields";
  }

  auto describe() const -> Description override {
    auto d = Describer<DropNullFieldsArgs, DropNullFields>{};
    d.parallelizable();
    auto fields
      = d.optional_variadic("fields", &DropNullFieldsArgs::fields, "field");
    d.optimization(&DropNullFieldsArgs::optimization);
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
    // `drop_null_fields` removes fields per row/null-pattern, never events,
    // and does not reorder ordered input. Predicates that reference a
    // potentially dropped field must stay behind us; independent predicates
    // may move upstream. Without explicit fields, every field may be dropped.
    return d.field_local(
      [=](DescribeCtx& ctx) -> Option<std::vector<ast::field_path>> {
        auto touched_fields = std::vector<ast::field_path>{};
        for (auto& value : ctx.get_all(fields)) {
          if (not value) {
            return None{};
          }
          auto selector = ast::field_path::try_from(*value);
          if (not selector or selector->path().empty()) {
            return None{};
          }
          touched_fields.push_back(std::move(*selector));
        }
        if (touched_fields.empty()) {
          return None{};
        }
        return touched_fields;
      });
  }
};

} // namespace tenzir::plugins::drop_null_fields

TENZIR_REGISTER_PLUGIN(tenzir::plugins::drop_null_fields::plugin)
