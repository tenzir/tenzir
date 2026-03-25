//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
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
class delay_operator final : public crtp_operator<delay_operator> {
public:
  delay_operator() = default;

  explicit delay_operator(located<std::string> field, double speed,
                          std::optional<time> start) noexcept
    : field_{std::move(field)}, speed_{speed}, start_{start} {
  }

  auto name() const -> std::string override {
    return "delay";
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto resolved_fields = std::unordered_map<type, std::optional<offset>>{};
    auto start = start_;
    const auto start_time = std::chrono::steady_clock::now();
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      const auto& layout = as<record_type>(slice.schema());
      auto resolved_field = resolved_fields.find(slice.schema());
      if (resolved_field == resolved_fields.end()) {
        const auto index
          = slice.schema().resolve_key_or_concept_once(field_.inner);
        if (not index) {
          diagnostic::warning("failed to resolve field `{}` for schema `{}`",
                              field_.inner, slice.schema())
            .primary(field_)
            .emit(ctrl.diagnostics());
          resolved_field = resolved_fields.emplace_hint(
            resolved_field, slice.schema(), std::nullopt);
        } else if (auto t = layout.field(*index).type; not is<time_type>(t)) {
          diagnostic::warning("field `{}` for schema `{}` has type `{}`",
                              field_.inner, slice.schema(), t.kind())
            .note("expected `{}`", type{time_type{}}.kind())
            .primary(field_)
            .emit(ctrl.diagnostics());
          resolved_field = resolved_fields.emplace_hint(
            resolved_field, slice.schema(), std::nullopt);
        } else {
          resolved_field = resolved_fields.emplace_hint(resolved_field,
                                                        slice.schema(), *index);
        }
      }
      TENZIR_ASSERT(resolved_field != resolved_fields.end());
      if (not resolved_field->second) {
        co_yield std::move(slice);
        continue;
      }
      size_t begin = 0;
      size_t end = 0;
      auto [_, array] = resolved_field->second->get(slice);
      for (auto&& element : values(
             time_type{},
             static_cast<const type_to_arrow_array_t<time_type>&>(*array))) {
        if (not element) {
          ++end;
          continue;
        }
        if (not start) [[unlikely]] {
          start = *element;
        }
        const auto anchor
          = *start
            + std::chrono::duration_cast<duration>(
              std::chrono::duration_cast<
                std::chrono::duration<double, duration::period>>(
                std::chrono::steady_clock::now() - start_time)
              * speed_);
        const auto delay = std::chrono::duration_cast<duration>(
          std::chrono::duration_cast<
            std::chrono::duration<double, duration::period>>(*element - anchor)
          / speed_);
        if (delay > duration::zero()) {
          co_yield subslice(slice, begin, end);
          ctrl.self().run_delayed_weak(delay, [&] {
            begin = end;
            ctrl.set_waiting(false);
          });
          ctrl.set_waiting(true);
          co_yield {};
        }
        ++end;
      }
      co_yield subslice(slice, begin, end);
    }
  }

  auto optimize(expression const& filter, event_order) const
    -> optimize_result override {
    return optimize_result{filter, event_order::ordered, copy()};
  }

  friend auto inspect(auto& f, delay_operator& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.delay.delay_operator")
      .fields(f.field("field", x.field_), f.field("speed", x.speed_),
              f.field("start", x.start_));
  }

private:
  located<std::string> field_;
  double speed_ = 1.0;
  std::optional<time> start_;
};

class delay_operator2 final : public crtp_operator<delay_operator2> {
public:
  delay_operator2() = default;

  explicit delay_operator2(ast::expression expr, double speed,
                           std::optional<time> start) noexcept
    : expr_{std::move(expr)}, speed_{speed}, start_{start} {
  }

  auto name() const -> std::string override {
    return "tql2.delay";
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto resolved_fields = std::unordered_map<type, std::optional<offset>>{};
    auto start = start_;
    const auto start_time = std::chrono::steady_clock::now();
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto sers = eval(expr_, slice, ctrl.diagnostics());
      for (auto& ser : sers.parts()) {
        auto* ptr = try_as<arrow::TimestampArray>(ser.array.get());
        if (not ptr) {
          if (ser.type.kind().is_not<null_type>()) {
            diagnostic::warning("expected `time`, got `{}`", ser.type.kind())
              .primary(expr_)
              .emit(ctrl.diagnostics());
          }
          co_yield std::move(slice);
          continue;
        }
        size_t begin = 0;
        size_t end = 0;
        const auto& array = *ptr;
        for (const auto&& element : values(time_type{}, array)) {
          if (not element) {
            ++end;
            continue;
          }
          if (not start) [[unlikely]] {
            start = *element;
          }
          const auto anchor
            = *start
              + std::chrono::duration_cast<duration>(
                std::chrono::duration_cast<
                  std::chrono::duration<double, duration::period>>(
                  std::chrono::steady_clock::now() - start_time)
                * speed_);
          const auto delay = std::chrono::duration_cast<duration>(
            std::chrono::duration_cast<
              std::chrono::duration<double, duration::period>>(*element
                                                               - anchor)
            / speed_);
          if (delay > duration::zero()) {
            co_yield subslice(slice, begin, end);
            ctrl.self().run_delayed_weak(delay, [&] {
              begin = end;
              ctrl.set_waiting(false);
            });
            ctrl.set_waiting(true);
            co_yield {};
          }
          ++end;
        }
        co_yield subslice(slice, begin, end);
      }
    }
  }

  auto optimize(expression const& filter, event_order) const
    -> optimize_result override {
    return optimize_result{filter, event_order::ordered, copy()};
  }

  friend auto inspect(auto& f, delay_operator2& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.delay.delay_operator2")
      .fields(f.field("expr", x.expr_), f.field("speed", x.speed_),
              f.field("start", x.start_));
  }

private:
  ast::expression expr_;
  double speed_ = 1.0;
  std::optional<time> start_;
};

struct DelayArgs {
  ast::expression expr;
  double speed = 1.0;
  std::optional<time> start;
};

class Delay final : public Operator<table_slice, table_slice> {
public:
  explicit Delay(DelayArgs args)
    : expr_{std::move(args.expr)}, speed_{args.speed}, start_{args.start} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (input.rows() == 0) {
      co_return;
    }
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
  std::optional<time> start_;
  std::optional<std::chrono::steady_clock::time_point> start_time_;
};

class plugin final : public virtual operator_plugin<delay_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto speed = std::optional<located<double>>{};
    auto start = std::optional<time>{};
    auto field = located<std::string>{};
    auto parser = argument_parser{"delay", "https://docs.tenzir.com/"
                                           "operators/delay"};
    parser.add("--speed", speed, "<factor>");
    parser.add("--start", start, "<time>");
    parser.add(field, "<field>");
    parser.parse(p);
    if (speed and speed->inner <= 0.0) {
      diagnostic::error("`--speed` must be greater than 0")
        .primary(speed.value())
        .throw_();
    }
    return std::make_unique<delay_operator>(std::move(field),
                                            speed ? speed->inner : 1.0, start);
  }
};

class plugin2 final : public virtual operator_plugin2<delay_operator2>,
                      public virtual OperatorPlugin {
  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto speed = std::optional<located<double>>{};
    auto start = std::optional<time>{};
    auto expr = ast::expression{};
    argument_parser2::operator_("delay")
      .positional("by", expr, "time")
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
    return std::make_unique<delay_operator2>(std::move(expr),
                                             speed ? speed->inner : 1.0, start);
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

TENZIR_REGISTER_PLUGIN(tenzir::plugins::delay::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::delay::plugin2)
