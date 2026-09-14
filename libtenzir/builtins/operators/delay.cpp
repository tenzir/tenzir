//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/async.hpp>
#include <tenzir/async/task.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/type.hpp>

#include <caf/typed_event_based_actor.hpp>

namespace tenzir::plugins::delay {

namespace {
struct DelayArgs {
  ast::expression expr;
  double speed = 1.0;
  Option<time> start;
};

class Delay final : public Operator<table_slice, table_slice> {
public:
  explicit Delay(DelayArgs args)
    : expr_{std::move(args.expr)}, speed_{args.speed}, start_{args.start} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (not start_time_) {
      start_time_ = std::chrono::steady_clock::now();
    }
    auto times_series = eval(expr_, input, ctx.dh());
    size_t begin = 0;
    size_t end = 0;
    for (auto& ser : times_series) {
      // validate type
      auto* times = try_as<arrow::TimestampArray>(ser.array.get());
      if (not times) {
        if (ser.type.kind().is_not<null_type>()) {
          diagnostic::warning("expected `time`, got `{}`", ser.type.kind())
            .primary(expr_)
            .emit(ctx.dh());
        }
        // include this series in the selected range
        end += ser.length();
        continue;
      }
      for (const auto& time : values3(*times)) {
        if (not time) {
          ++end;
          continue;
        }
        if (not start_) [[unlikely]] {
          start_ = *time;
        }
        TENZIR_ASSERT(start_time_);
        // compute needed delay
        const auto elapsed = std::chrono::steady_clock::now() - *start_time_;
        const auto anchor = *start_ + duration_cast<duration>(elapsed * speed_);
        const auto delay = duration_cast<std::chrono::steady_clock::duration>(
          duration_cast<std::chrono::duration<double>>(*time - anchor)
          / speed_);
        // if needed, push & sleep
        if (delay > duration::zero()) {
          if (end > begin) {
            // emit data points before current
            co_await push(subslice(input, begin, end));
            begin = end;
          }
          co_await sleep_for(delay);
        }
        // advance
        ++end;
      }
    }
    co_await push(subslice(input, begin, end));
  }

  auto snapshot(Serde& serde) -> void override {
    TENZIR_UNUSED(serde);
    // start and start_time are not stored in the snapshot, which can leads to
    // confusing results when this operator is restored from snapshot.
    // This is because we have no easy way to carry the steady_clock time point
    // between potentially different machines or just machine reboots.
    // Also, this operator is more for demonstration purposes and does not need
    // such amount of snapshot rigidity.
  }

private:
  ast::expression expr_;
  double speed_;
  Option<time> start_;
  Option<std::chrono::steady_clock::time_point> start_time_;
};

class plugin2 final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "delay";
  }

  auto describe() const -> Description override {
    auto d = Describer<DelayArgs, Delay>{};
    d.positional("by", &DelayArgs::expr, "time");
    auto speed = d.named_optional("speed", &DelayArgs::speed);
    d.named("start", &DelayArgs::start);
    d.validate([speed](DescribeCtx& ctx) -> Empty {
      TRY(auto value, ctx.get(speed));
      if (value <= 0.0) {
        diagnostic::error("`speed` must be greater than 0")
          .primary(ctx.get_location(speed).value())
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::delay

TENZIR_REGISTER_PLUGIN(tenzir::plugins::delay::plugin2)
