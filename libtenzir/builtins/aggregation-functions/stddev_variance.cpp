//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/nova/aggregation/statistics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::stddev_variance {

namespace {

enum class mode {
  stddev,
  variance,
};

// TODO: This can be cleaned up probably
class stddev_variance_instance final : public aggregation_instance {
public:
  stddev_variance_instance(ast::expression expr, mode m)
    : mode_{m}, expr_{std::move(expr)} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    if (state_ == state::failed) {
      return;
    }
    for (auto& arg : eval(expr_, input, ctx)) {
      auto f = detail::overload{
        [](const arrow::NullArray&) {},
        [&]<class T>(const T& array)
          requires numeric_type<type_from_arrow_t<T>>
                     or std::same_as<T, arrow::DurationArray>
        {
          if constexpr (std::same_as<T, arrow::DurationArray>) {
            if (state_ != state::dur and state_ != state::none) {
              diagnostic::warning("got incompatible types `number` and `{}`",
                                  arg.type.kind())
                .primary(expr_)
                .emit(ctx);
              state_ = state::failed;
              return;
            }
            if (mode_ == mode::variance) {
              diagnostic::warning("expected `int`, `uint` or `double` got `{}`",
                                  arg.type.kind())
                .primary(expr_)
                .emit(ctx);
              state_ = state::failed;
              return;
            }
            state_ = state::dur;
          } else {
            if (state_ != state::numeric and state_ != state::none) {
              diagnostic::warning("got incompatible types `duration` and `{}`",
                                  arg.type.kind())
                .primary(expr_)
                .emit(ctx);
              state_ = state::failed;
              return;
            }
            state_ = state::numeric;
          }
          for (auto i = int64_t{}; i < array.length(); ++i) {
            if (array.IsValid(i)) {
              const auto x = static_cast<double>(array.Value(i));
              if constexpr (std::is_same_v<type_from_arrow_t<T>, double_type>) {
                if (std::isnan(x)) {
                  continue;
                }
              }
              count_ += 1;
              mean_ += (x - mean_) / count_;
              mean_squared_ += ((x * x) - mean_squared_) / count_;
            }
          }
        },
        [&](const auto&) {
          if (mode_ == mode::variance) {
            diagnostic::warning("expected `int`, `uint` or `double` got `{}`",
                                arg.type.kind())
              .primary(expr_)
              .emit(ctx);
          } else {
            diagnostic::warning("expected `int`, `uint`, `double` or "
                                "`duration`, "
                                "got `{}`",
                                arg.type.kind())
              .primary(expr_)
              .emit(ctx);
          }
          state_ = state::failed;
        }};
      match(*arg.array, f);
    }
  }

  auto get() const -> data override {
    if (count_ == 0) {
      return data{};
    }
    const auto variance = mean_squared_ - (mean_ * mean_);
    const auto result = mode_ == mode::stddev ? std::sqrt(variance) : variance;
    switch (state_) {
      case state::none:
      case state::failed:
        return data{};
      case state::dur:
        return duration{static_cast<duration::rep>(result)};
      case state::numeric:
        return result;
    }
    TENZIR_UNREACHABLE();
  }

  auto save() const -> chunk_ptr override {
    auto fbb = flatbuffers::FlatBufferBuilder{};
    const auto fb_state = [&] {
      switch (state_) {
        case state::none:
          return fbs::aggregation::StddevVarianceState::None;
        case state::failed:
          return fbs::aggregation::StddevVarianceState::Failed;
        case state::dur:
          return fbs::aggregation::StddevVarianceState::Duration;
        case state::numeric:
          return fbs::aggregation::StddevVarianceState::Numeric;
      }
      TENZIR_UNREACHABLE();
    }();
    const auto fb_mean = fbs::aggregation::CreateStddevVariance(
      fbb, mean_, mean_squared_, count_, fb_state);
    fbb.Finish(fb_mean);
    return chunk::make(fbb.Release());
  }

  auto restore(chunk_ptr chunk) noexcept -> bool override {
    const auto name = mode_ == mode::stddev ? "stddev" : "variance";
    const auto fb
      = flatbuffer<fbs::aggregation::StddevVariance>::make(std::move(chunk));
    if (not fb) {
      TENZIR_WARN("failed to restore `{}` aggregation instance: invalid "
                  "FlatBuffer",
                  name);
      return false;
    }
    mean_ = (*fb)->result();
    mean_squared_ = (*fb)->result_squared();
    count_ = (*fb)->count();
    switch ((*fb)->state()) {
      case fbs::aggregation::StddevVarianceState::None:
        state_ = state::none;
        return true;
      case fbs::aggregation::StddevVarianceState::Failed:
        state_ = state::failed;
        return true;
      case fbs::aggregation::StddevVarianceState::Duration:
        state_ = state::dur;
        return true;
      case fbs::aggregation::StddevVarianceState::Numeric:
        state_ = state::numeric;
        return true;
    }
    TENZIR_WARN(
      "failed to restore `{}` aggregation instance: unknown state value", name);
    return false;
  }

  auto reset() -> void override {
    mean_ = {};
    mean_squared_ = {};
    count_ = {};
    state_ = state::none;
  }

private:
  double mean_ = {};
  double mean_squared_ = {};
  size_t count_ = {};
  mode mode_ = {};
  enum class state { none, failed, dur, numeric } state_{state::none};
  ast::expression expr_;
};

struct StddevVarianceArgs {
  nova::ValueArgument x;
};

/// The running mean and mean of squares, shared by the accumulator and the
/// list kernel. Only `stddev` accepts durations.
template <mode Mode>
class Dispersion {
public:
  template <class T>
  auto add(T value) -> void {
    auto const x = nova_statistics::to_double(value);
    count_ += 1;
    auto const n = static_cast<double>(count_);
    mean_ += (x - mean_) / n;
    mean_squared_ += ((x * x) - mean_squared_) / n;
  }

  auto get(nova_statistics::NumericKind const& kind) const -> nova::Data {
    if (count_ == 0) {
      return nova::Data{};
    }
    auto const variance = mean_squared_ - (mean_ * mean_);
    return nova_statistics::make_result(
      kind, Mode == mode::stddev ? std::sqrt(variance) : variance);
  }

private:
  double mean_ = 0.0;
  double mean_squared_ = 0.0;
  size_t count_ = 0;
};

template <mode Mode>
class StddevVarianceFunction final {
public:
  static constexpr auto allow_duration = Mode == mode::stddev;

  static auto eval(StddevVarianceArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    return nova_statistics::eval_statistic<Dispersion<Mode>>(
      args.x, frame, allow_duration, [] {
        return Dispersion<Mode>{};
      });
  }

  auto update(StddevVarianceArgs const& args, nova::EvalFrame frame) -> void {
    kind_.visit(args.x.data, frame.mask(), args.x.source, frame,
                [&](auto value) {
                  dispersion_.add(value);
                });
  }

  auto get() const -> nova::Data {
    return dispersion_.get(kind_);
  }

  auto reset() -> void {
    kind_ = nova_statistics::NumericKind{allow_duration};
    dispersion_ = {};
  }

private:
  nova_statistics::NumericKind kind_{allow_duration};
  Dispersion<Mode> dispersion_;
};

template <mode Mode>
class plugin : public virtual aggregation_plugin,
               public virtual nova::AggregationPlugin {
  auto name() const -> std::string override {
    return Mode == mode::stddev ? "stddev" : "variance";
  };

  auto describe() const -> nova::AggregationDescription override {
    auto d = nova::AggregationDescriber<StddevVarianceArgs,
                                        StddevVarianceFunction<Mode>>{};
    d.positional("x", &StddevVarianceArgs::x,
                 Mode == mode::stddev ? "number|duration" : "number");
    return std::move(d).finish();
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_aggregation(function_invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr,
                      Mode == mode::stddev ? "number|duration" : "number")
          .parse(inv, ctx));
    return std::make_unique<stddev_variance_instance>(std::move(expr), Mode);
  }
};

using stddev_plugin = plugin<mode::stddev>;
using variance_plugin = plugin<mode::variance>;

} // namespace

} // namespace tenzir::plugins::stddev_variance

TENZIR_REGISTER_PLUGIN(tenzir::plugins::stddev_variance::stddev_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::stddev_variance::variance_plugin)
