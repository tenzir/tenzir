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

class timeshift_operator2 final : public crtp_operator<timeshift_operator2> {
public:
  timeshift_operator2() = default;

  explicit timeshift_operator2(ast::field_path selector, double speed,
                               std::optional<time> start) noexcept
    : speed_{speed}, selector_{std::move(selector)}, start_{start} {
  }

  auto name() const -> std::string override {
    return "tql2.timeshift";
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto first_time = std::optional<time>{};
    auto start = start_;
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      for (auto& s : eval(selector_.inner(), slice, ctrl.diagnostics())) {
        if (s.type.kind().is_not<time_type>()) {
          if (s.type.kind().is_not<null_type>()) {
            diagnostic::warning("expected `time`, got `{}`", s.type.kind())
              .primary(selector_)
              .emit(ctrl.diagnostics());
          }
          co_yield std::move(slice);
          continue;
        }
        const auto& array = as<arrow::TimestampArray>(*s.array);
        auto b = time_type::make_arrow_builder(arrow_memory_pool());
        auto offset = int64_t{0};
        for (const auto& value : values(time_type{}, array)) {
          if (not value) {
            check(b->AppendNull());
            continue;
          }
          if (not first_time) [[unlikely]] {
            first_time = value;
          }
          if (not start) [[unlikely]] {
            start = value;
          }
          const auto shifted = *start + (*value - *first_time) / speed_;
          check(b->Append(shifted.time_since_epoch().count()));
        }
        auto times = series{time_type{}, finish(*b)};
        auto sliced = subslice(slice, offset, offset + times.length());
        co_yield assign(selector_, std::move(times), sliced,
                        ctrl.diagnostics());
      }
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    if (speed_ == 1.0 and not start_) {
      // If this operator is a no-op we can just remove it during optimization.
      return optimize_result{filter, order, nullptr};
    }
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, timeshift_operator2& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.timeshift.timeshift_operator2")
      .fields(f.field("selector", x.selector_), f.field("speed", x.speed_),
              f.field("start", x.start_));
  }

private:
  double speed_{1.0};
  ast::field_path selector_;
  std::optional<time> start_;
};

struct TimeshiftArgs {
  ast::field_path selector;
  double speed = 1.0;
  std::optional<time> start;
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
    if (input.rows() == 0) {
      co_return;
    }
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
  std::optional<time> start_;
  std::optional<time> first_time_;
};

struct plugin2 : operator_plugin2<timeshift_operator2>, virtual OperatorPlugin {
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto speed = std::optional<located<double>>{};
    auto start = std::optional<time>{};
    auto selector = ast::field_path{};
    argument_parser2::operator_("timeshift")
      .positional("field", selector, "time")
      .named("speed", speed)
      .named("start", start)
      .parse(inv, ctx)
      .ignore();
    if (speed and speed->inner <= 0.0) {
      diagnostic::error("`speed` must be greater than 0")
        .primary(speed.value())
        .emit(ctx);
      return failure::promise();
    }
    return std::make_unique<timeshift_operator2>(
      std::move(selector), speed ? speed->inner : 1.0, start);
  }

  auto describe() const -> Description override {
    auto d = Describer<TimeshiftArgs, Timeshift>{};
    d.positional("field", &TimeshiftArgs::selector, "time");
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
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::timeshift

TENZIR_REGISTER_PLUGIN(tenzir::plugins::timeshift::plugin2)
