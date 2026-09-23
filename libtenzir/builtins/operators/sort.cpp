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
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/comparison.hpp>
#include <tenzir/nova/data_array_builder.hpp>
#include <tenzir/nova/eval.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/nova/fundamental_array_builder.hpp>
#include <tenzir/nova/record_array_builder.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/type.hpp>

#include <arrow/compute/api_vector.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <ranges>

namespace tenzir::plugins::sort {

namespace {

// -- New executor-based TQL2 implementation -----------------------------------

struct SortArgs {
  std::vector<ast::expression> exprs;
  location keyword;
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

class SortNova final : public Operator<nova::Events, nova::Events> {
public:
  explicit SortNova(SortArgs args) {
    if (args.exprs.empty()) {
      keys_.push_back({
        .expr = ast::expression{ast::this_{location::unknown}},
        .evaluator = {},
        .chunks = {},
      });
      return;
    }
    keys_.reserve(args.exprs.size());
    for (auto& arg : args.exprs) {
      auto* unary = try_as<ast::unary_expr>(arg);
      if (unary and unary->op == ast::unary_op::neg) {
        keys_.push_back({
          .expr = std::move(unary->expr),
          .reverse = true,
          .evaluator = {},
          .chunks = {},
        });
      } else {
        keys_.push_back({
          .expr = std::move(arg),
          .evaluator = {},
          .chunks = {},
        });
      }
    }
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    for (auto& key : keys_) {
      auto evaluator = nova::Evaluator::make(
        std::move(key.expr), nova::InstantiateCtx{ctx.dh(), ctx.reg()});
      if (not evaluator) {
        co_return;
      }
      key.evaluator.emplace(std::move(*evaluator));
    }
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    nova::storage::for_each_true(input.mask, [&](nova::storage::Index row) {
      indices_.push_back({events_.size(), row});
    });
    for (auto& key : keys_) {
      key.chunks.push_back(key.evaluator->eval(input, nova::EvalCtx{ctx.dh()}));
    }
    events_.push_back(std::move(input));
    co_return;
  }

  auto finalize(Push<nova::Events>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    std::stable_sort(
      indices_.begin(), indices_.end(),
      [&](Index const& lhs, Index const& rhs) {
        for (auto const& key : keys_) {
          auto const lhs_value = key.chunks[lhs.batch].get(lhs.row);
          auto const rhs_value = key.chunks[rhs.batch].get(rhs.row);
          auto const lhs_null = is_null(lhs_value);
          auto const rhs_null = is_null(rhs_value);
          if (lhs_null and rhs_null) {
            continue;
          }
          if (lhs_null) {
            return false;
          }
          if (rhs_null) {
            return true;
          }
          auto const relation = compare(lhs_value, rhs_value);
          if (relation != std::weak_ordering::equivalent) {
            return (relation == std::weak_ordering::less) != key.reverse;
          }
        }
        return false;
      });
    auto builder = EventsBuilder{};
    for (auto const& index : indices_) {
      builder.append(events_[index.batch], index.row);
      if (builder.length()
          == detail::narrow<nova::storage::Index>(
            defaults::import::table_slice_size)) {
        co_await push(std::exchange(builder, EventsBuilder{}).finish());
      }
    }
    if (builder.length() > 0) {
      co_await push(std::move(builder).finish());
    }
    co_return FinalizeBehavior::done;
  }

  auto snapshot(Serde&) -> void override {
    // Buffered Nova events and evaluated keys are not serializable yet.
    TENZIR_TODO();
  }

private:
  static auto is_null(nova::RowView<nova::Data> const& value) -> bool {
    return match(value, []<class T>(nova::RowView<T>) {
      return std::same_as<T, nova::Null>;
    });
  }

  static auto rank(nova::RowView<nova::Data> const& value) -> size_t {
    return match(value, []<class T>(nova::RowView<T>) {
      if constexpr (std::same_as<T, nova::Null>) {
        return size_t{12};
      } else if constexpr (std::same_as<T, bool>) {
        return size_t{0};
      } else if constexpr (std::same_as<T, int64_t>) {
        return size_t{1};
      } else if constexpr (std::same_as<T, uint64_t>) {
        return size_t{2};
      } else if constexpr (std::same_as<T, double>) {
        return size_t{3};
      } else if constexpr (std::same_as<T, duration>) {
        return size_t{4};
      } else if constexpr (std::same_as<T, time>) {
        return size_t{5};
      } else if constexpr (std::same_as<T, std::string_view>) {
        return size_t{6};
      } else if constexpr (std::same_as<T, ip>) {
        return size_t{7};
      } else if constexpr (std::same_as<T, subnet>) {
        return size_t{8};
      } else if constexpr (std::same_as<T, nova::List>) {
        return size_t{9};
      } else if constexpr (std::same_as<T, nova::Record>) {
        return size_t{10};
      } else {
        static_assert(std::same_as<T, blob_view>);
        return size_t{11};
      }
    });
  }

  static auto compare(nova::RowView<nova::Data> const& lhs,
                      nova::RowView<nova::Data> const& rhs)
    -> std::weak_ordering {
    return match(
      std::tie(lhs, rhs),
      []<class L, class R>(nova::RowView<L> lhs,
                           nova::RowView<R> rhs) -> std::weak_ordering {
        constexpr auto lhs_numeric
          = concepts::one_of<L, int64_t, uint64_t, double>;
        constexpr auto rhs_numeric
          = concepts::one_of<R, int64_t, uint64_t, double>;
        if constexpr (lhs_numeric and rhs_numeric) {
          auto const relation = nova::_::compare_numbers(*lhs, *rhs);
          if (relation == std::partial_ordering::unordered) {
            if constexpr (std::same_as<L, double> and std::same_as<R, double>) {
              return std::isnan(*lhs) == std::isnan(*rhs)
                       ? std::weak_ordering::equivalent
                     : std::isnan(*lhs) ? std::weak_ordering::greater
                                        : std::weak_ordering::less;
            } else if constexpr (std::same_as<L, double>) {
              return std::weak_ordering::greater;
            } else {
              return std::weak_ordering::less;
            }
          }
          return relation < 0   ? std::weak_ordering::less
                 : relation > 0 ? std::weak_ordering::greater
                                : std::weak_ordering::equivalent;
        } else if constexpr (not std::same_as<L, R>) {
          return rank(lhs) <=> rank(rhs);
        } else if constexpr (std::same_as<L, nova::Null>) {
          return std::weak_ordering::equivalent;
        } else if constexpr (std::same_as<L, nova::List>) {
          auto lhs_it = lhs.begin();
          auto rhs_it = rhs.begin();
          while (lhs_it != lhs.end() and rhs_it != rhs.end()) {
            if (auto result = compare(*lhs_it, *rhs_it);
                result != std::weak_ordering::equivalent) {
              return result;
            }
            ++lhs_it;
            ++rhs_it;
          }
          if (lhs_it == lhs.end() and rhs_it == rhs.end()) {
            return std::weak_ordering::equivalent;
          }
          return lhs_it == lhs.end() ? std::weak_ordering::less
                                     : std::weak_ordering::greater;
        } else if constexpr (std::same_as<L, nova::Record>) {
          auto lhs_it = lhs.begin();
          auto rhs_it = rhs.begin();
          while (lhs_it != lhs.end() and rhs_it != rhs.end()) {
            auto [lhs_name, lhs_value] = *lhs_it;
            auto [rhs_name, rhs_value] = *rhs_it;
            if (auto result = lhs_name <=> rhs_name;
                result != std::weak_ordering::equivalent) {
              return result;
            }
            if (auto result = compare(lhs_value, rhs_value);
                result != std::weak_ordering::equivalent) {
              return result;
            }
            ++lhs_it;
            ++rhs_it;
          }
          if (lhs_it == lhs.end() and rhs_it == rhs.end()) {
            return std::weak_ordering::equivalent;
          }
          return lhs_it == lhs.end() ? std::weak_ordering::less
                                     : std::weak_ordering::greater;
        } else if constexpr (std::same_as<L, blob_view>) {
          if (std::ranges::lexicographical_compare(*lhs, *rhs)) {
            return std::weak_ordering::less;
          }
          if (std::ranges::lexicographical_compare(*rhs, *lhs)) {
            return std::weak_ordering::greater;
          }
          return std::weak_ordering::equivalent;
        } else if constexpr (requires {
                               *lhs < *rhs;
                               *lhs > *rhs;
                             }) {
          if (nova::compare<ast::binary_op::lt>(*lhs, *rhs)) {
            return std::weak_ordering::less;
          }
          if (nova::compare<ast::binary_op::gt>(*lhs, *rhs)) {
            return std::weak_ordering::greater;
          }
          return std::weak_ordering::equivalent;
        } else {
          static_assert(sizeof(L) == 0, "missing Nova sort comparator");
        }
      });
  }

  struct EventsBuilder {
    auto append(nova::Events const& input, nova::storage::Index row) -> void {
      auto output = data.record();
      for (auto [name, value] : input.data.get(row)) {
        nova::append_row(output.field(name), value);
      }
      names.data(*input.meta.name.get(row));
      import_times.data(*input.meta.import_time.get(row));
      internal.data(*input.meta.internal.get(row));
    }

    auto length() const -> nova::storage::Index {
      return data.length();
    }

    auto finish() && -> nova::Events {
      auto result = data.finish();
      auto mask = nova::storage::BitMap{result.length(), true};
      return {
        std::move(result),
        std::move(mask),
        {
          .name = names.finish(),
          .import_time = import_times.finish(),
          .internal = internal.finish(),
        },
      };
    }

    nova::ArrayBuilder<nova::Record> data;
    nova::ArrayBuilder<nova::String> names;
    nova::ArrayBuilder<nova::Time> import_times;
    nova::ArrayBuilder<nova::Bool> internal;
  };

  struct Key {
    ast::expression expr;
    bool reverse = false;
    Option<nova::Evaluator> evaluator;
    std::vector<nova::Array<nova::Data>> chunks;
  };

  struct Index {
    size_t batch;
    nova::storage::Index row;
  };

  std::vector<Key> keys_;
  std::vector<nova::Events> events_;
  std::vector<Index> indices_;
};

class plugin2 final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "sort";
  }

  auto describe() const -> Description override {
    auto d = Describer<SortArgs, Sort, SortNova>{};
    d.operator_location(&SortArgs::keyword);
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
