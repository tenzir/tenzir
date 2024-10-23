//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_time_utils.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/tenzir/si.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/util/tdigest.h>

#include <random>

namespace tenzir::plugins::numeric {

namespace {

class round_use final : public function_use {
public:
  round_use(ast::expression value, std::optional<located<duration>> spec)
    : value_{std::move(value)}, spec_{std::move(spec)} {
  }

  auto run(evaluator eval, session ctx) const -> series override {
    auto value = located{eval(value_), value_.get_location()};
    if (not spec_) {
      // round(<number>)
      return round_without_spec(std::move(value), ctx);
    }
    // round(<duration>, <duration>)
    // round(x, 1h) -> round to multiples of 1h
    // round(<time>, <duration>)
    // round(x, 1h) -> round so that time is multiples of 1h (for UTC timezone?)
    return round_with_spec(std::move(value), spec_.value(), ctx);
  }

private:
  static auto round_with_spec(located<series> value, located<duration> spec,
                              session ctx) -> series {
    auto f = detail::overload{
      [&](const arrow::DurationArray& array) -> series {
        auto b
          = duration_type::make_arrow_builder(arrow::default_memory_pool());
        check(b->Reserve(array.length()));
        for (auto i = int64_t{0}; i < array.length(); i++) {
          if (array.IsNull(i)) {
            check(b->AppendNull());
            continue;
          }
          const auto val = array.Value(i);
          const auto count = spec.inner.count();
          const auto rem = val % std::abs(count);
          const auto delta = rem * 2 < count ? -rem : count - rem;
          check(b->Append(val + delta));
        }
        return {duration_type{}, finish(*b)};
      },
      [&](const arrow::TimestampArray& array) -> series {
        auto rounded_array
          = check(arrow::compute::RoundTemporal(
                    array, make_round_temporal_options(spec.inner)))
              .array_as<arrow::TimestampArray>();
        return {time_type{}, rounded_array};
      },
      [&](const auto&) {
        diagnostic::warning("round(_, _) is not implemented for {}",
                            value.inner.type)
          .primary(value)
          .emit(ctx);
        return series::null(value.inner.type, value.inner.length());
      }};
    return caf::visit(f, *value.inner.array);
  }

  static auto round_without_spec(located<series> value, session ctx) -> series {
    auto length = value.inner.length();
    auto null = [&] {
      auto b = arrow::Int64Builder{};
      check(b.AppendNulls(length));
      return finish(b);
    };
    auto f = detail::overload{
      [&](const arrow::NullArray&) {
        return null();
      },
      [](const arrow::Int64Array& arg) {
        return std::make_shared<arrow::Int64Array>(arg.data());
      },
      [&](const arrow::DoubleArray& arg) {
        auto b = arrow::Int64Builder{};
        check(b.Reserve(length));
        for (auto row = int64_t{0}; row < length; ++row) {
          if (arg.IsNull(row)) {
            check(b.AppendNull());
          } else {
            // TODO: NaN, inf, ...
            auto result = std::llround(arg.Value(row));
            check(b.Append(result));
          }
        }
        return finish(b);
      },
      [&]<concepts::one_of<arrow::DurationArray, arrow::TimestampArray> T>(
        const T&) {
        diagnostic::warning("`round` with duration requires second argument")
          .primary(value)
          .hint("for example `round(x, 1h)`")
          .emit(ctx);
        return null();
      },
      [&](const auto&) {
        diagnostic::warning("`round` expected `int64` or `double`, got `{}`",
                            value.inner.type.kind())
          // TODO: Wrong location.
          .primary(value)
          .emit(ctx);
        return null();
      },
    };
    return series{int64_type{}, caf::visit(f, *value.inner.array)};
  }

  ast::expression value_;
  std::optional<located<duration>> spec_;
};

class round final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.round";
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto value = ast::expression{};
    auto spec = std::optional<located<duration>>{};
    TRY(argument_parser2::function("round")
          .add(value, "<value>")
          .add(spec, "<spec>")
          .parse(inv, ctx));
    if (spec && spec->inner.count() == 0) {
      diagnostic::error("resolution must not be 0")
        .primary(spec.value())
        .emit(ctx);
      return failure::promise();
    }
    return std::make_unique<round_use>(std::move(value), std::move(spec));
  }
};

class sqrt final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.sqrt";
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(
      argument_parser2::function("sqrt").add(expr, "<number>").parse(inv, ctx));
    return function_use::make([expr = std::move(expr)](evaluator eval,
                                                       session ctx) -> series {
      auto value = eval(expr);
      auto compute = [&](const arrow::DoubleArray& x) {
        auto b = arrow::DoubleBuilder{};
        check(b.Reserve(x.length()));
        for (auto y : x) {
          if (not y) {
            check(b.AppendNull());
            continue;
          }
          auto z = *y;
          if (z < 0.0) {
            // TODO: Warning?
            check(b.AppendNull());
            continue;
          }
          check(b.Append(std::sqrt(z)));
        }
        return finish(b);
      };
      auto f = detail::overload{
        [&](const arrow::DoubleArray& value) {
          return compute(value);
        },
        [&](const arrow::Int64Array& value) {
          // TODO: Conversation should be automatic (if not
          // part of the kernel).
          auto b = arrow::DoubleBuilder{};
          check(b.Reserve(value.length()));
          for (auto y : value) {
            if (y) {
              check(b.Append(static_cast<double>(*y)));
            } else {
              check(b.AppendNull());
            }
          }
          return compute(*finish(b));
        },
        [&](const arrow::NullArray& value) {
          return series::null(double_type{}, value.length()).array;
        },
        [&](const auto&) {
          // TODO: Think about what we want and generalize this.
          diagnostic::warning("expected `number`, got `{}`", value.type.kind())
            .primary(expr)
            .docs("https://docs.tenzir.com/functions/sqrt")
            .emit(ctx);
          auto b = arrow::DoubleBuilder{};
          check(b.AppendNulls(value.length()));
          return finish(b);
        },
      };
      return {double_type{}, caf::visit(f, *value.array)};
    });
  }
};

class random final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.random";
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    argument_parser2::function("random").parse(inv, ctx).ignore();
    return function_use::make([](evaluator eval, session ctx) -> series {
      TENZIR_UNUSED(ctx);
      auto b = arrow::DoubleBuilder{};
      check(b.Reserve(eval.length()));
      auto engine = std::default_random_engine{std::random_device{}()};
      auto dist = std::uniform_real_distribution<double>{0.0, 1.0};
      for (auto i = int64_t{0}; i < eval.length(); ++i) {
        check(b.Append(dist(engine)));
      }
      return {double_type{}, finish(b)};
    });
  }
};

class count_instance final : public aggregation_instance {
public:
  explicit count_instance(std::optional<ast::expression> expr)
    : expr_{std::move(expr)} {
  }

  void update(const table_slice& input, session ctx) override {
    if (not expr_) {
      count_ += detail::narrow<int64_t>(input.rows());
      return;
    }
    auto arg = eval(*expr_, input, ctx);
    count_ += arg.array->length() - arg.array->null_count();
  }

  auto finish() -> data override {
    return count_;
  }

private:
  std::optional<ast::expression> expr_;
  int64_t count_ = 0;
};

class count final : public aggregation_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.count";
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = std::optional<ast::expression>{};
    TRY(
      argument_parser2::function("count").add(expr, "<expr>").parse(inv, ctx));
    return std::make_unique<count_instance>(std::move(expr));
  }
};

class quantile_instance final : public aggregation_instance {
public:
  quantile_instance(ast::expression expr, double quantile, uint32_t delta,
                    uint32_t buffer_size)
    : expr_{std::move(expr)}, quantile_{quantile}, digest_{delta, buffer_size} {
  }

  void update(const table_slice& input, session ctx) override {
    if (state_ == state::failed) {
      return;
    }
    auto arg = eval(expr_, input, ctx);
    auto f = detail::overload{
      [&]<concepts::one_of<double_type, int64_type, uint64_type> Type>(
        const Type& ty) {
        if (state_ != state::numeric and state_ != state::none) {
          diagnostic::warning("got incompatible types `number` and `{}`",
                              arg.type.kind())
            .primary(expr_)
            .emit(ctx);
          state_ = state::failed;
          return;
        }
        state_ = state::numeric;
        auto& array = caf::get<type_to_arrow_array_t<Type>>(*arg.array);
        for (auto value : values(ty, array)) {
          if (value) {
            digest_.NanAdd(*value);
          }
        }
      },
      [&](const duration_type& ty) {
        if (state_ != state::dur and state_ != state::none) {
          diagnostic::warning("got incompatible types `duration` and `{}`",
                              arg.type.kind())
            .primary(expr_)
            .emit(ctx);
          state_ = state::failed;
          return;
        }
        state_ = state::dur;
        for (auto value :
             values(ty, caf::get<arrow::DurationArray>(*arg.array))) {
          if (value) {
            digest_.Add(value->count());
          }
        }
      },
      [&](const null_type&) {
        // Silently ignore nulls, like we do above.
      },
      [&](const auto&) {
        diagnostic::warning("expected `int`, `uint`, `double` or `duration`, "
                            "got `{}`",
                            arg.type.kind())
          .primary(expr_)
          .emit(ctx);
        state_ = state::failed;
      },
    };
    caf::visit(f, arg.type);
  }

  auto finish() -> data override {
    switch (state_) {
      case state::none:
      case state::failed:
        return data{};
      case state::dur:
        return duration{
          static_cast<duration::rep>(digest_.Quantile(quantile_))};
      case state::numeric:
        return digest_.Quantile(quantile_);
    }
    TENZIR_UNREACHABLE();
  }

private:
  ast::expression expr_;
  double quantile_;
  enum class state { none, failed, dur, numeric } state_{};
  arrow::internal::TDigest digest_;
};

class quantile final : public aggregation_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.quantile";
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    // TODO: Reconsider whether we want a default here. Maybe positional?
    auto quantile_opt = std::optional<located<double>>{};
    auto delta_opt = std::optional<located<int64_t>>{};
    auto buffer_size_opt = std::optional<located<int64_t>>{};
    TRY(argument_parser2::function("quantile")
          .add(expr, "expr")
          .add("q", quantile_opt)
          // TODO: This is a test for hidden parameters.
          .add("_delta", delta_opt)
          .add("_buffer_size", buffer_size_opt)
          .parse(inv, ctx));
    // TODO: Type conversion? Probably not necessary here, but maybe elsewhere.
    // TODO: This is too much manual labor.
    auto quantile = 0.5;
    if (quantile_opt) {
      if (quantile_opt->inner < 0.0 || quantile_opt->inner > 1.0) {
        diagnostic::error("expected quantile to be in [0.0, 1.0]")
          .primary(*quantile_opt)
          .emit(ctx);
      }
      quantile = quantile_opt->inner;
    }
    // TODO: This function probably already exists. If not, it should.
    auto try_narrow = [](int64_t x) -> std::optional<uint32_t> {
      if (0 <= x && x <= std::numeric_limits<uint32_t>::max()) {
        return static_cast<uint32_t>(x);
      }
      return std::nullopt;
    };
    auto delta = uint32_t{100};
    if (delta_opt) {
      if (auto narrowed = try_narrow(delta_opt->inner)) {
        delta = *narrowed;
      } else {
        diagnostic::error("expected delta to fit in a uint32")
          .primary(*delta_opt)
          .emit(ctx);
      }
    }
    auto buffer_size = uint32_t{500};
    if (buffer_size_opt) {
      if (auto narrowed = try_narrow(buffer_size_opt->inner)) {
        buffer_size = *narrowed;
      } else {
        diagnostic::error("expected buffer size to fit in a uint32")
          .primary(*buffer_size_opt)
          .emit(ctx);
      }
    }
    return std::make_unique<quantile_instance>(expr, quantile, delta,
                                               buffer_size);
  }
};

class median final : public aggregation_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.median";
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    // TODO: Reconsider whether we want a default here. Maybe positional?
    auto delta_opt = std::optional<located<int64_t>>{};
    auto buffer_size_opt = std::optional<located<int64_t>>{};
    TRY(argument_parser2::function("median")
          .add(expr, "expr")
          // TODO: This is a test for hidden parameters.
          .add("_delta", delta_opt)
          .add("_buffer_size", buffer_size_opt)
          .parse(inv, ctx));
    // TODO: This function probably already exists. If not, it should.
    auto try_narrow = [](int64_t x) -> std::optional<uint32_t> {
      if (0 <= x && x <= std::numeric_limits<uint32_t>::max()) {
        return static_cast<uint32_t>(x);
      }
      return std::nullopt;
    };
    auto delta = uint32_t{100};
    if (delta_opt) {
      if (auto narrowed = try_narrow(delta_opt->inner)) {
        delta = *narrowed;
      } else {
        diagnostic::error("expected delta to fit in a uint32")
          .primary(*delta_opt)
          .emit(ctx);
      }
    }
    auto buffer_size = uint32_t{500};
    if (buffer_size_opt) {
      if (auto narrowed = try_narrow(buffer_size_opt->inner)) {
        buffer_size = *narrowed;
      } else {
        diagnostic::error("expected buffer size to fit in a uint32")
          .primary(*buffer_size_opt)
          .emit(ctx);
      }
    }
    return std::make_unique<quantile_instance>(expr, 0.5, delta, buffer_size);
  }
};

} // namespace

} // namespace tenzir::plugins::numeric

TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::round)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::sqrt)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::random)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::count)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::quantile)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::median)
