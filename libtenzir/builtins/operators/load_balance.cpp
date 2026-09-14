//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/source.hpp"
#include "tenzir/tql2/exec.hpp"
#include "tenzir/view.hpp"

#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <caf/anon_mail.hpp>

#include <algorithm>
#include <deque>

namespace tenzir::plugins::load_balance {

namespace {

auto as_constant_kind(data const& value) -> ast::constant::kind {
  return match(value, []<class T>(T const& x) -> ast::constant::kind {
    if constexpr (std::same_as<T, pattern>) {
      TENZIR_UNREACHABLE();
    } else {
      return x;
    }
  });
}

struct LoadBalanceArgs {
  located<list> over;
  located<ir::pipeline> pipe;
  let_id over_id;
};

class LoadBalance final : public Operator<table_slice, void> {
public:
  explicit LoadBalance(LoadBalanceArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (started_) {
      co_return;
    }
    started_ = true;
    workers_.assign(args_.over.inner.size(), Worker{});
    active_workers_ = workers_.size();
    for (auto i = size_t{0}; i < args_.over.inner.size(); ++i) {
      auto pipe = args_.pipe.inner;
      pipe.bind(args_.over_id, as_constant_kind(args_.over.inner[i]));
      if (not co_await ctx.plan_and_spawn_sub<table_slice>(
            data{detail::narrow<int64_t>(i)}, std::move(pipe))) {
        done_ = true;
        co_return;
      }
    }
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    auto const rows = input.rows();
    while (active_workers_ > 0) {
      auto worker = select_worker();
      TENZIR_ASSERT(worker);
      auto sub = ctx.get_sub(detail::narrow<int64_t>(*worker));
      if (not sub) {
        mark_inactive(*worker);
        continue;
      }
      auto& pipe = as<SubHandle<table_slice>>(*sub);
      auto result = co_await pipe.push(std::move(input));
      if (result.is_ok()) {
        workers_[*worker].rows_assigned += rows;
        co_return;
      }
      input = std::move(result).unwrap_err();
      mark_inactive(*worker);
    }
    done_ = true;
  }

  auto finish_sub(SubKeyView key, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    auto key_data = materialize(key);
    auto* index = try_as<int64_t>(key_data);
    TENZIR_ASSERT(index);
    mark_inactive(detail::narrow<size_t>(*index));
    co_return;
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    for (auto i = size_t{0}; i < workers_.size(); ++i) {
      if (not workers_[i].active) {
        continue;
      }
      if (auto sub = ctx.get_sub(detail::narrow<int64_t>(i))) {
        auto& pipe = as<SubHandle<table_slice>>(*sub);
        co_await pipe.close();
      }
      mark_inactive(i);
    }
    done_ = true;
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  struct Worker {
    uint64_t rows_assigned = 0;
    bool active = true;
  };

  auto select_worker() const -> Option<size_t> {
    auto result = Option<size_t>{None{}};
    for (auto i = size_t{0}; i < workers_.size(); ++i) {
      if (not workers_[i].active) {
        continue;
      }
      if (not result
          or workers_[i].rows_assigned < workers_[*result].rows_assigned) {
        result = i;
      }
    }
    return result;
  }

  auto mark_inactive(size_t index) -> void {
    TENZIR_ASSERT(index < workers_.size());
    if (not workers_[index].active) {
      return;
    }
    workers_[index].active = false;
    TENZIR_ASSERT(active_workers_ > 0);
    --active_workers_;
    if (active_workers_ == 0) {
      done_ = true;
    }
  }

  LoadBalanceArgs args_;
  std::vector<Worker> workers_;
  size_t active_workers_ = 0;
  bool started_ = false;
  bool done_ = false;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "load_balance";
  }

  auto describe() const -> Description override {
    auto d = Describer<LoadBalanceArgs, LoadBalance>{};
    auto over = d.positional("over", &LoadBalanceArgs::over, "list");
    auto pipe = d.pipeline(&LoadBalanceArgs::pipe, SubOptimize::from_downstream,
                           {{"over", &LoadBalanceArgs::over_id}});
    d.validate([over, pipe](DescribeCtx& ctx) -> Empty {
      if (auto entries = ctx.get(over); entries and entries->inner.empty()) {
        diagnostic::error("expected list to not be empty")
          .primary(entries->source)
          .emit(ctx);
      }
      TRY(auto p, ctx.get(pipe));
      auto output = p.inner.infer_type(tag_v<table_slice>, ctx);
      if (output.is_error()) {
        return {};
      }
      if (output->is_not<void>()) {
        diagnostic::error("pipeline must currently end with a sink")
          .primary(p.source)
          .emit(ctx);
      }
      return {};
    });
    return d.optimize(
      [](DescribeCtx& ctx, ir::OptimizeRequest req) -> Optimization {
        auto touched = ast::ExprRefs{.let_ids = ctx.pipeline_let_ids()};
        auto [independent, dependent]
          = ir::split_filter_by_dependents(std::move(req.filter), touched);
        return {
          .order = EventOrder::unordered,
          .filter_upstream = std::move(independent),
          .filter_self = std::move(dependent),
          .projection_upstream = std::move(req.projection),
        };
      });
  }
};

} // namespace

} // namespace tenzir::plugins::load_balance

TENZIR_REGISTER_PLUGIN(tenzir::plugins::load_balance::plugin)
