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

namespace tenzir::plugins::mean {

namespace {

class mean_instance final : public aggregation_instance {
public:
  explicit mean_instance(ast::expression expr) : expr_{std::move(expr)} {
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
              diagnostic::warning(
                "expected `int`, `uint` or `double`, got `{}`", arg.type.kind())
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
              if constexpr (std::same_as<T, arrow::DoubleArray>) {
                if (std::isnan(array.Value(i))) {
                  continue;
                }
              }
              count_ += 1;
              mean_ += (static_cast<double>(array.Value(i)) - mean_) / count_;
            }
          }
        },
        [&](const auto&) {
          diagnostic::warning("expected types `int`, `uint`, "
                              "`double` or `duration`, got `{}`",
                              arg.type.kind())
            .primary(expr_)
            .emit(ctx);
          state_ = state::failed;
        }};
      match(*arg.array, f);
    }
  }

  auto get() const -> data override {
    switch (state_) {
      case state::none:
      case state::failed:
        return data{};
      case state::dur:
        return duration{static_cast<duration::rep>(mean_)};
      case state::numeric:
        return count_ ? data{mean_} : data{};
    }
    TENZIR_UNREACHABLE();
  }

  auto save() const -> chunk_ptr override {
    auto fbb = flatbuffers::FlatBufferBuilder{};
    const auto fb_state = [&] {
      switch (state_) {
        case state::none:
          return fbs::aggregation::MeanState::None;
        case state::failed:
          return fbs::aggregation::MeanState::Failed;
        case state::dur:
          return fbs::aggregation::MeanState::Duration;
        case state::numeric:
          return fbs::aggregation::MeanState::Numeric;
      }
      TENZIR_UNREACHABLE();
    }();
    const auto fb_mean
      = fbs::aggregation::CreateMean(fbb, mean_, count_, fb_state);
    fbb.Finish(fb_mean);
    return chunk::make(fbb.Release());
  }

  auto restore(chunk_ptr chunk, session ctx) -> void override {
    const auto fb = flatbuffer<fbs::aggregation::Mean>::make(std::move(chunk));
    if (not fb) {
      diagnostic::warning("invalid FlatBuffer")
        .note("failed to restore `mean` aggregation instance")
        .emit(ctx);
      return;
    }
    mean_ = (*fb)->result();
    count_ = (*fb)->count();
    switch ((*fb)->state()) {
      case fbs::aggregation::MeanState::None:
        state_ = state::none;
        return;
      case fbs::aggregation::MeanState::Failed:
        state_ = state::failed;
        return;
      case fbs::aggregation::MeanState::Duration:
        state_ = state::dur;
        return;
      case fbs::aggregation::MeanState::Numeric:
        state_ = state::numeric;
        return;
    }
    diagnostic::warning("unknown `state` value")
      .note("failed to restore `mean` aggregation instance")
      .emit(ctx);
  }

  auto reset() -> void override {
    mean_ = {};
    count_ = {};
    state_ = state::none;
  }

private:
  ast::expression expr_;
  enum class state { none, failed, dur, numeric } state_{};
  double mean_{};
  size_t count_{};
};

class plugin : public virtual aggregation_plugin {
  auto name() const -> std::string override {
    return "mean";
  };

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "number|duration")
          .parse(inv, ctx));
    return std::make_unique<mean_instance>(std::move(expr));
  }
};

} // namespace

} // namespace tenzir::plugins::mean

TENZIR_REGISTER_PLUGIN(tenzir::plugins::mean::plugin)
