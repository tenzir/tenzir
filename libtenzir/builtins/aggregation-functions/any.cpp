//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/aggregation_function.hpp>
#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::any {

namespace {

class any_function final : public aggregation_function {
public:
  explicit any_function(type input_type) noexcept
    : aggregation_function(std::move(input_type)) {
    // nop
  }

private:
  [[nodiscard]] type output_type() const override {
    TENZIR_ASSERT(caf::holds_alternative<bool_type>(input_type()));
    return input_type();
  }

  void add(const data_view& view) override {
    using view_type = tenzir::view<bool>;
    if (caf::holds_alternative<caf::none_t>(view)) {
      return;
    }
    if (!any_) {
      any_ = materialize(as<view_type>(view));
    } else {
      any_ = *any_ || as<view_type>(view);
    }
  }

  void add(const arrow::Array& array) override {
    const auto& bool_array = as<type_to_arrow_array_t<bool_type>>(array);
    if (!any_) {
      any_ = bool_array.true_count() > 0;
    } else {
      any_ = *any_ || bool_array.true_count() > 0;
    }
  }

  [[nodiscard]] caf::expected<data> finish() && override {
    return data{any_};
  }

  std::optional<bool> any_ = {};
};

class any_instance final : public aggregation_instance {
public:
  explicit any_instance(ast::expression expr) : expr_{std::move(expr)} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    if (state_ == state::failed) {
      return;
    }
    auto arg = eval(expr_, input, ctx);
    auto f = detail::overload{
      [&](const arrow::NullArray&) {
        state_ = state::nulled;
      },
      [&](const arrow::BooleanArray& array) {
        any_ = any_ or array.true_count() > 0;
        if (array.null_count() > 0) {
          state_ = state::nulled;
        }
      },
      [&](auto&&) {
        diagnostic::warning("expected type `bool`, got `{}`", arg.type.kind())
          .primary(expr_)
          .emit(ctx);
        state_ = state::failed;
      }};
    caf::visit(f, *arg.array);
  }

  auto get() const -> data override {
    switch (state_) {
      case state::none:
        return any_;
      case state::nulled:
        return any_ ? true : data{};
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
    const auto fb_any_all = fbs::aggregation::CreateAnyAll(fbb, any_, fb_state);
    fbb.Finish(fb_any_all);
    return chunk::make(fbb.Release());
  }

  auto restore(chunk_ptr chunk, session ctx) -> void override {
    const auto fb
      = flatbuffer<fbs::aggregation::AnyAll>::make(std::move(chunk));
    if (not fb) {
      diagnostic::warning("invalid FlatBuffer")
        .note("failed to restore `any` aggregation instance")
        .emit(ctx);
      return;
    }
    any_ = (*fb)->result();
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
      .note("failed to restore `any` aggregation instance")
      .emit(ctx);
  }

private:
  ast::expression expr_;
  bool any_{false};
  enum class state : int8_t { none, failed, nulled } state_{state::none};
};

class plugin : public virtual aggregation_function_plugin,
               public virtual aggregation_plugin {
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "any";
  };

  [[nodiscard]] caf::expected<std::unique_ptr<aggregation_function>>
  make_aggregation_function(const type& input_type) const override {
    if (caf::holds_alternative<bool_type>(input_type)) {
      return std::make_unique<any_function>(input_type);
    }
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("any aggregation function does not "
                                       "support type {}",
                                       input_type));
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name()).add(expr, "<expr>").parse(inv, ctx));
    return std::make_unique<any_instance>(std::move(expr));
  }

  auto aggregation_default() const -> data override {
    return false;
  }
};

} // namespace

} // namespace tenzir::plugins::any

TENZIR_REGISTER_PLUGIN(tenzir::plugins::any::plugin)
