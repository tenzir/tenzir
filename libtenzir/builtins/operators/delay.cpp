//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/detail/alarm_clock.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/type.hpp>

#include <caf/typed_event_based_actor.hpp>

namespace tenzir::plugins::delay {

namespace {

class delay_operator final : public crtp_operator<delay_operator> {
public:
  delay_operator() = default;

  explicit delay_operator(std::string field, double speed,
                          std::optional<time> start) noexcept
    : field_{std::move(field)}, speed_{speed}, start_{start} {
  }

  auto name() const -> std::string override {
    return "delay";
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto alarm_clock = ctrl.self().spawn(detail::make_alarm_clock);
    auto resolved_fields = std::unordered_map<type, std::optional<offset>>{};
    auto start = start_;
    const auto start_time = std::chrono::steady_clock::now();
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      const auto& layout = caf::get<record_type>(slice.schema());
      auto resolved_field = resolved_fields.find(slice.schema());
      if (resolved_field == resolved_fields.end()) {
        const auto index = slice.schema().resolve_key_or_concept_once(field_);
        if (not index) {
          diagnostic::warning("failed to resolve field `{}` for schema `{}`",
                              field_, slice.schema())
            .note("from `{}`", name())
            .emit(ctrl.diagnostics());
          resolved_field = resolved_fields.emplace_hint(
            resolved_field, slice.schema(), std::nullopt);
        } else if (auto t = layout.field(*index).type;
                   not caf::holds_alternative<time_type>(t)) {
          diagnostic::warning("field `{}` for schema `{}` has type `{}`",
                              field_, slice.schema(), t.kind())
            .note("expected `{}`", type{time_type{}}.kind())
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
          ctrl.self()
            .request(alarm_clock, caf::infinite, delay)
            .await(
              [&]() {
                begin = end;
              },
              [&ctrl, deadline = *element](const caf::error& err) {
                diagnostic::error("failed to delay until `{}`: {}", deadline,
                                  err)
                  .emit(ctrl.diagnostics());
              });
          co_yield {};
        }
        ++end;
      }
      co_yield subslice(slice, begin, end);
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    return optimize_result::order_invariant(*this, order);
  }

  friend auto inspect(auto& f, delay_operator& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.delay.delay_operator")
      .fields(f.field("field", x.field_), f.field("speed", x.speed_),
              f.field("start", x.start_));
  }

private:
  std::string field_ = {};
  double speed_ = 1.0;
  std::optional<time> start_ = {};
};

class plugin final : public virtual operator_plugin<delay_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto speed = std::optional<double>{};
    auto start = std::optional<time>{};
    auto field = std::string{};
    auto parser = argument_parser{"delay", "https://docs.tenzir.com/"
                                           "operators/delay"};
    parser.add("--speed", speed, "<factor>");
    parser.add("--start", start, "<time>");
    parser.add(field, "<field>");
    parser.parse(p);
    if (speed and *speed <= 0.0) {
      diagnostic::error("`--speed` must be greater than 0")
        .note("from `{}`", name())
        .throw_();
    }
    return std::make_unique<delay_operator>(std::move(field),
                                            speed.value_or(1.0), start);
  }
};

} // namespace

} // namespace tenzir::plugins::delay

TENZIR_REGISTER_PLUGIN(tenzir::plugins::delay::plugin)
