//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::all {

namespace {

class all_instance final : public aggregation_instance {
public:
  explicit all_instance(ast::expression expr) : expr_{std::move(expr)} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    if (state_ == state::failed) {
      return;
    }
    for (auto& arg : eval(expr_, input, ctx)) {
      auto f = detail::overload{
        [&](const arrow::NullArray&) {
          state_ = state::nulled;
        },
        [&](const arrow::BooleanArray& array) {
          all_ = all_ and array.false_count() == 0;
          if (array.null_count() > 0) {
            state_ = state::nulled;
          }
        },
        [&](const auto&) {
          diagnostic::warning("expected type `bool`, got `{}`", arg.type.kind())
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
        return all_;
      case state::nulled:
        return all_ ? data{} : false;
      case state::failed:
        return data{};
    }
    TENZIR_UNREACHABLE();
  }

  auto save() const -> chunk_ptr override {
    auto fbb = flatbuffers::FlatBufferBuilder{};
    const auto fb_state = [&] {
      switch (state_) {
        case state::none:
          return fbs::aggregation::AnyAllState::None;
        case state::failed:
          return fbs::aggregation::AnyAllState::Failed;
        case state::nulled:
          return fbs::aggregation::AnyAllState::Nulled;
      }
      TENZIR_UNREACHABLE();
    }();
    const auto fb_any_all = fbs::aggregation::CreateAnyAll(fbb, all_, fb_state);
    fbb.Finish(fb_any_all);
    return chunk::make(fbb.Release());
  }

  auto restore(chunk_ptr chunk, session ctx) -> void override {
    const auto fb
      = flatbuffer<fbs::aggregation::AnyAll>::make(std::move(chunk));
    if (not fb) {
      diagnostic::warning("invalid FlatBuffer")
        .note("failed to restore `all` aggregation instance")
        .emit(ctx);
      return;
    }
    all_ = (*fb)->result();
    switch ((*fb)->state()) {
      case fbs::aggregation::AnyAllState::None:
        state_ = state::none;
        return;
      case fbs::aggregation::AnyAllState::Failed:
        state_ = state::failed;
        return;
      case fbs::aggregation::AnyAllState::Nulled:
        state_ = state::nulled;
        return;
    }
    diagnostic::warning("unknown `state` value")
      .note("failed to restore `all` aggregation instance")
      .emit(ctx);
  }

  auto reset() -> void override {
    all_ = true;
    state_ = state::none;
  }

private:
  ast::expression expr_;
  bool all_{true};
  enum class state : int8_t { none, failed, nulled } state_{state::none};
};

class plugin final : public virtual aggregation_plugin {
  auto name() const -> std::string override {
    return "all";
  };

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "bool")
          .parse(inv, ctx));
    return std::make_unique<all_instance>(std::move(expr));
  }
};

} // namespace

} // namespace tenzir::plugins::all

TENZIR_REGISTER_PLUGIN(tenzir::plugins::all::plugin)
