//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/async.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>
#include <tenzir/type.hpp>

namespace tenzir::plugins::timeshift {

namespace {

struct TimeshiftArgs {
  ast::field_path selector;
  double speed = 1.0;
  Option<time> start;
};

class Timeshift final : public Operator<table_slice, table_slice> {
public:
  explicit Timeshift(TimeshiftArgs args)
    : selector_{std::move(args.selector)},
      speed_{args.speed},
      start_{args.start} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    // eval
    auto s = eval(selector_, input, ctx.dh());
    // validate type
    if (s.type.kind().is_not<time_type>()) {
      if (s.type.kind().is_not<null_type>()) {
        diagnostic::warning("expected `time`, got `{}`", s.type.kind())
          .primary(selector_)
          .emit(ctx.dh());
      }
      co_await push(std::move(input));
      co_return;
    }
    // map values
    const auto& array = as<arrow::TimestampArray>(*s.array);
    auto b = time_type::make_arrow_builder(arrow_memory_pool());
    for (const auto& value : values3(array)) {
      if (not value) {
        check(b->AppendNull());
        continue;
      }
      if (not first_time_) [[unlikely]] {
        first_time_ = value;
      }
      if (not start_) [[unlikely]] {
        start_ = value;
      }
      const auto offset = std::chrono::duration_cast<duration>(
        (*value - *first_time_) / speed_);
      const auto shifted = (*start_ + offset).time_since_epoch().count();
      check(b->Append(shifted));
    }
    auto times = series{time_type{}, finish(*b)};
    // output
    auto output = assign(selector_, std::move(times), input, ctx.dh());
    co_await push(std::move(output));
  }

  auto snapshot(Serde& serde) -> void override {
    serde("start_", start_);
    serde("first_time_", first_time_);
  }

private:
  ast::field_path selector_;
  double speed_;
  Option<time> start_;
  Option<time> first_time_;
};

struct plugin2 : virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "timeshift";
  }

  auto describe() const -> Description override {
    auto d = Describer<TimeshiftArgs, Timeshift>{};
    auto selector = d.positional("field", &TimeshiftArgs::selector, "time");
    auto speed = d.named_optional("speed", &TimeshiftArgs::speed);
    d.named("start", &TimeshiftArgs::start);
    d.validate([speed](DescribeCtx& ctx) -> Empty {
      TRY(auto value, ctx.get(speed));
      if (value <= 0.0) {
        diagnostic::error("`speed` must be greater than 0")
          .primary(ctx.get_location(speed).value())
          .emit(ctx);
      }
      return {};
    });
    // The first event's time defines the shift origin, so predicates must not
    // cross `timeshift` and ordered input is required.
    return d.optimize(
      [=](DescribeCtx& ctx, ir::OptimizeRequest req) -> Optimization {
        auto projection = std::move(req.projection);
        // `timeshift` reads and rewrites the given field in place. Retain it
        // even when downstream does not need it: the first value defines the
        // origin, and a type mismatch warns.
        if (auto selector_path = ctx.get(selector)) {
          ir::add_to_projection(projection, *selector_path);
        } else {
          projection = None{};
        }
        // All predicates stay behind us. A carried limit still passes through
        // when there are none: `timeshift` is 1:1, so the first N outputs stem
        // from exactly the first N inputs.
        auto limit = req.filter.empty() ? req.limit : Option<uint64_t>{};
        return {
          .order = EventOrder::ordered,
          .filter_self = std::move(req.filter),
          .limit_upstream = limit,
          .projection_upstream = std::move(projection),
        };
      });
  }
};

} // namespace

} // namespace tenzir::plugins::timeshift

TENZIR_REGISTER_PLUGIN(tenzir::plugins::timeshift::plugin2)
