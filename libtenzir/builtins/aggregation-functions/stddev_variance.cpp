//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
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

  auto restore(chunk_ptr chunk, session ctx) -> void override {
    const auto fb
      = flatbuffer<fbs::aggregation::StddevVariance>::make(std::move(chunk));
    if (not fb) {
      diagnostic::warning("invalid FlatBuffer")
        .note("failed to restore `{}` aggregation instance",
              mode_ == mode::stddev ? "stddev" : "variance")
        .emit(ctx);
      return;
    }
    mean_ = (*fb)->result();
    mean_squared_ = (*fb)->result_squared();
    count_ = (*fb)->count();
    switch ((*fb)->state()) {
      case fbs::aggregation::StddevVarianceState::None:
        state_ = state::none;
        return;
      case fbs::aggregation::StddevVarianceState::Failed:
        state_ = state::failed;
        return;
      case fbs::aggregation::StddevVarianceState::Duration:
        state_ = state::dur;
        return;
      case fbs::aggregation::StddevVarianceState::Numeric:
        state_ = state::numeric;
        return;
    }
    diagnostic::warning("unknown `state` value")
      .note("failed to restore `{}` aggregation instance",
            mode_ == mode::stddev ? "stddev" : "variance")
      .emit(ctx);
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

template <mode Mode>
class plugin : public virtual aggregation_plugin {
  auto name() const -> std::string override {
    return Mode == mode::stddev ? "stddev" : "variance";
  };

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_aggregation(invocation inv, session ctx) const
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
