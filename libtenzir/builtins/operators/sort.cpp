//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/builtins/sort.hpp"

#include "tenzir/compile_ctx.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <algorithm>

namespace tenzir::plugins::sort {

namespace {

auto describe_sort() -> Description {
  auto d = Describer<SortArgs>{};
  d.name("sort");
  d.operator_location(&SortArgs::keyword);
  d.optional_variadic("expr", &SortArgs::exprs, "any");
  return d.only_arguments();
}

using SortArguments = OperatorArguments<SortArgs, describe_sort>;

class SortIr final : public ir::Operator {
public:
  SortIr() = default;

  explicit SortIr(SortArguments args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "sort";
  }

  auto main_location() const -> location override {
    return args_.main_location();
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    return args_.substitute(ctx, instantiate);
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<element_type_tag> override {
    if (input.is_not<nova::Events>() and input.is_not<table_slice>()) {
      diagnostic::error("operator expects events")
        .primary(main_location())
        .emit(dh);
      return failure::promise();
    }
    return input;
  }

  auto spawn(element_type_tag input) const -> AnyOperator override {
    if (input.is<nova::Events>()) {
      return make_sort(args_.get(), limit_);
    }
    TENZIR_ASSERT(input.is<table_slice>());
    return legacy::make_sort(args_.get());
  }

  auto optimize(ir::OptimizeRequest req,
                ir::OptimizeCtx const&) && -> ir::OptimizeResult override {
    if (req.order == EventOrder::unordered) {
      // With no ordering requirement, all downstream hints still apply.
      return {
        .filter = std::move(req.filter),
        .order = EventOrder::unordered,
        .replacement = {},
        .limit = req.limit,
        .projection = std::move(req.projection),
      };
    }
    if (req.limit) {
      limit_ = limit_ ? std::min(*limit_, *req.limit) : req.limit;
    }
    // Every predicate commutes with the retained sort and moves before its
    // candidate selection. This bespoke IR therefore needs no generic
    // limit-only runtime binding. Never pass a prefix limit through a sort.
    for (auto const& filter : req.filter) {
      ir::add_refs_to_projection(req.projection, filter);
    }
    auto args = args_.get();
    if (args.exprs.empty()) {
      req.projection = None{};
    }
    for (auto const& expr : args.exprs) {
      ir::add_refs_to_projection(req.projection, expr);
    }
    auto replacement = ir::pipeline{};
    replacement.operators.push_back(std::move(*this).move());
    return {
      .filter = std::move(req.filter),
      .order = EventOrder::unordered,
      .replacement = std::move(replacement),
      .projection = std::move(req.projection),
    };
  }

  friend auto inspect(auto& f, SortIr& x) -> bool {
    return f.object(x).fields(f.field("args", x.args_),
                              f.field("limit", x.limit_));
  }

private:
  SortArguments args_;
  Option<uint64_t> limit_;
};

class plugin2 final : public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "sort";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    TRY(auto args, SortArguments::parse(std::move(inv), ctx));
    return SortIr{std::move(args)};
  }
};

} // namespace

} // namespace tenzir::plugins::sort

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sort::plugin2)
TENZIR_REGISTER_PLUGIN(
  tenzir::inspection_plugin<tenzir::ir::Operator, tenzir::plugins::sort::SortIr>)
