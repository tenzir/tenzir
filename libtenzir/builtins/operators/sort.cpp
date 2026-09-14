//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/option.hpp"

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/numeric/integral.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/type.hpp>

#include <arrow/compute/api_vector.h>

#include <algorithm>
#include <ranges>

namespace tenzir::plugins::sort {

namespace {

// -- New executor-based TQL2 implementation -----------------------------------

struct SortArgs {
  std::vector<ast::expression> exprs;
};

class Sort final : public Operator<table_slice, table_slice> {
public:
  explicit Sort(SortArgs args) {
    if (args.exprs.empty()) {
      keys_.emplace_back(ast::expression{ast::this_{location::unknown}});
      return;
    }
    keys_.reserve(args.exprs.size());
    for (auto& arg : args.exprs) {
      auto* unary = try_as<ast::unary_expr>(arg);
      if (unary and unary->op == ast::unary_op::neg) {
        keys_.emplace_back(std::move(unary->expr), true);
      } else {
        keys_.emplace_back(std::move(arg));
      }
    }
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    auto const length = detail::narrow<int64_t>(input.rows());
    indices_.reserve(indices_.size() + input.rows());
    for (auto i = int64_t{0}; i < length; ++i) {
      indices_.emplace_back(events_.size(), i);
    }
    for (auto& key : keys_) {
      key.chunks.emplace_back(eval(key.expr, input, ctx.dh()));
    }
    events_.emplace_back(std::move(input));
    co_return;
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    if (indices_.empty()) {
      co_return FinalizeBehavior::done;
    }
    // TODO: If all chunks for a sort key evaluate to the same type, we can
    // choose a faster path where we do not need to evaluate the sort key's
    // type for each row individually.
    std::ranges::sort(indices_, [&](std::pair<size_t, int64_t> const& lhs,
                                    std::pair<size_t, int64_t> const& rhs) {
      for (auto const& key : keys_) {
        auto const& lhs_chunk = key.chunks[lhs.first];
        auto const& rhs_chunk = key.chunks[rhs.first];
        auto const lhs_value = lhs_chunk.view3_at(lhs.second);
        auto const rhs_value = rhs_chunk.view3_at(rhs.second);
        auto const lhs_null = is<caf::none_t>(lhs_value);
        auto const rhs_null = is<caf::none_t>(rhs_value);
        if (lhs_null and rhs_null) {
          continue;
        }
        if (lhs_null) {
          return false;
        }
        if (rhs_null) {
          return true;
        }
        auto const relation = weak_order(lhs_value, rhs_value);
        if (relation != std::weak_ordering::equivalent) {
          return (relation == std::weak_ordering::less) != key.reverse;
        }
      }
      return false;
    });
    // Assemble result, rebatching for efficiency.
    auto batch = std::vector<table_slice>{};
    auto batch_rows = size_t{0};
    for (auto const& [slice_idx, event_idx] : indices_) {
      if (not batch.empty()
          and batch.back().schema() != events_[slice_idx].schema()) {
        co_await push(concatenate(std::exchange(batch, {})));
        batch_rows = 0;
      }
      if (batch_rows >= defaults::import::table_slice_size) {
        co_await push(concatenate(std::exchange(batch, {})));
        batch_rows = 0;
      }
      // TODO: This excessive slicing is quite bad for performance. We could
      // merge consecutive entries from the same slice.
      batch.emplace_back(
        subslice(events_[slice_idx], event_idx, event_idx + 1));
      batch_rows += 1;
    }
    if (not batch.empty()) {
      co_await push(concatenate(std::move(batch)));
    }
    co_return FinalizeBehavior::done;
  }

private:
  struct Key {
    ast::expression expr;
    bool reverse = false;
    std::vector<multi_series> chunks;
  };

  std::vector<Key> keys_;
  std::vector<table_slice> events_;
  std::vector<std::pair<size_t, int64_t>> indices_;
};

class plugin2 final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "sort";
  }

  auto describe() const -> Description override {
    auto d = Describer<SortArgs, Sort>{};
    d.optional_variadic("expr", &SortArgs::exprs, "any");
    return d.optimize([](DescribeCtx&, EventOrder order,
                         ir::OptimizeFilter filter) -> Optimization {
      return {
        // invariant to order and filters
        .order = EventOrder::unordered,
        .filter_upstream = std::move(filter),
        // drop if downstream does not care about order
        .drop = order == EventOrder::unordered,
      };
    });
  }
};

} // namespace

} // namespace tenzir::plugins::sort

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sort::plugin2)
