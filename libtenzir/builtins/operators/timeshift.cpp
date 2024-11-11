//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>
#include <tenzir/type.hpp>

namespace tenzir::plugins::timeshift {

namespace {

/// timeshifts the specifed fields from the input.
class timeshift_operator final : public crtp_operator<timeshift_operator> {
public:
  timeshift_operator() = default;

  explicit timeshift_operator(std::string field, double speed,
                              std::optional<time> start) noexcept
    : field_{std::move(field)}, speed_{speed}, start_{start} {
  }

  auto name() const -> std::string override {
    return "timeshift";
  }

  auto
  operator()(generator<table_slice> input,
             operator_control_plane& ctrl) const -> generator<table_slice> {
    auto resolved_fields = std::unordered_map<type, std::optional<offset>>{};
    auto start = start_;
    auto first_time = std::optional<time>{};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      const auto& layout = as<record_type>(slice.schema());
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
        } else if (auto t = layout.field(*index).type; not is<time_type>(t)) {
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
      auto transform_fn = [&](struct record_type::field field,
                              std::shared_ptr<arrow::Array> array) noexcept
        -> indexed_transformation::result_type {
        TENZIR_ASSERT(is<time_type>(field.type));
        auto builder = series_builder{};
        for (auto element : values(
               time_type{},
               static_cast<const type_to_arrow_array_t<time_type>&>(*array))) {
          if (not element) {
            builder.null();
            continue;
          }
          if (not first_time) [[unlikely]] {
            first_time = *element;
            if (not start) {
              start = *element;
            }
          }
          builder.data(*start
                       + std::chrono::duration_cast<duration>(
                         (*element - *first_time) / speed_));
        }
        auto arrays = builder.finish();
        TENZIR_ASSERT(arrays.size() == 1);
        TENZIR_ASSERT(is<time_type>(arrays[0].type));
        return {{field, std::move(arrays[0].array)}};
      };
      auto transformations = std::vector<indexed_transformation>{
        {*resolved_field->second, transform_fn},
      };
      co_yield transform_columns(slice, transformations);
    }
  }

  auto optimize(expression const& filter,
                event_order order) const -> optimize_result override {
    if (speed_ == 1.0 and not start_) {
      // If this operator is a no-op we can just remove it during optimization.
      return optimize_result{filter, order, nullptr};
    }
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, timeshift_operator& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.timeshift.timeshift_operator")
      .fields(f.field("field", x.field_), f.field("speed", x.speed_),
              f.field("start", x.start_));
  }

private:
  std::string field_ = {};
  double speed_ = 1.0;
  std::optional<time> start_ = {};
};

class timeshift_operator2 final : public crtp_operator<timeshift_operator2> {
public:
  timeshift_operator2() = default;

  explicit timeshift_operator2(ast::simple_selector selector, double speed,
                               std::optional<time> start) noexcept
    : speed_{speed}, selector_{std::move(selector)}, start_{start} {
  }

  auto name() const -> std::string override {
    return "tql2.timeshift";
  }

  auto
  operator()(generator<table_slice> input,
             operator_control_plane& ctrl) const -> generator<table_slice> {
    auto first_time = std::optional<time>{};
    auto start = start_;
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield std::move(slice);
        continue;
      }
      auto s = eval(selector_.inner(), slice, ctrl.diagnostics());
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
      auto b = time_type::make_arrow_builder(arrow::default_memory_pool());
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
      co_yield assign(selector_, {time_type{}, finish(*b)}, slice,
                      ctrl.diagnostics());
    }
  }

  auto optimize(expression const& filter,
                event_order order) const -> optimize_result override {
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
  ast::simple_selector selector_;
  std::optional<time> start_;
};

class plugin final : public virtual operator_plugin<timeshift_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto speed = std::optional<double>{};
    auto start = std::optional<time>{};
    auto field = std::string{};
    auto parser = argument_parser{"timeshift", "https://docs.tenzir.com/"
                                               "operators/timeshift"};
    parser.add("--speed", speed, "<factor>");
    parser.add("--start", start, "<time>");
    parser.add(field, "<field>");
    parser.parse(p);
    if (speed and *speed <= 0.0) {
      diagnostic::error("`--speed` must be greater than 0")
        .note("from `{}`", name())
        .throw_();
    }
    return std::make_unique<timeshift_operator>(std::move(field),
                                                speed.value_or(1.0), start);
  }
};

struct plugin2 : operator_plugin2<timeshift_operator2> {
  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto speed = std::optional<located<double>>{};
    auto start = std::optional<time>{};
    auto selector = ast::simple_selector{};
    argument_parser2::operator_("timeshift")
      .add(selector, "<selector>")
      .add("speed", speed)
      .add("start", start)
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
};

} // namespace

} // namespace tenzir::plugins::timeshift

TENZIR_REGISTER_PLUGIN(tenzir::plugins::timeshift::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::timeshift::plugin2)
