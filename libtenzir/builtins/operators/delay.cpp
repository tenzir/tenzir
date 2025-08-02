//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
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
  located<std::string> field_ = {};
  double speed_ = 1.0;
  std::optional<time> start_ = {};
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
  std::optional<time> start_ = {};
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

class plugin2 final : public virtual operator_plugin2<delay_operator2> {
  auto make(invocation inv, session ctx) const
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
};

} // namespace

} // namespace tenzir::plugins::delay

TENZIR_REGISTER_PLUGIN(tenzir::plugins::delay::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::delay::plugin2)
