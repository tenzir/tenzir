//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/aggregation_function.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::all {

namespace {

class all_function final : public aggregation_function {
public:
  explicit all_function(type input_type) noexcept
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
    if (!all_) {
      all_ = materialize(caf::get<view_type>(view));
    } else {
      all_ = *all_ && caf::get<view_type>(view);
    }
  }

  void add(const arrow::Array& array) override {
    const auto& bool_array = caf::get<type_to_arrow_array_t<bool_type>>(array);
    if (!all_) {
      all_ = bool_array.false_count() == 0;
    } else {
      all_ = *all_ && bool_array.false_count() == 0;
    }
  }

  [[nodiscard]] caf::expected<data> finish() && override {
    return data{all_};
  }

  std::optional<bool> all_ = {};
};

class all_instance final : public aggregation_instance {
public:
  explicit all_instance(ast::expression expr) : expr_{std::move(expr)} {
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
        all_ = all_ and array.false_count() == 0;
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

  auto finish() -> data override {
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

private:
  ast::expression expr_;
  bool all_{true};
  enum class state : int8_t { none, failed, nulled } state_{state::none};
};

class plugin final : public virtual aggregation_function_plugin,
                     public virtual aggregation_plugin {
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "all";
  };

  [[nodiscard]] caf::expected<std::unique_ptr<aggregation_function>>
  make_aggregation_function(const type& input_type) const override {
    if (caf::holds_alternative<bool_type>(input_type)) {
      return std::make_unique<all_function>(input_type);
    }
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("all aggregation function does not "
                                       "support type {}",
                                       input_type));
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name()).add(expr, "<expr>").parse(inv, ctx));
    return std::make_unique<all_instance>(std::move(expr));
  }

  auto aggregation_default() const -> data override {
    return true;
  }
};

} // namespace

} // namespace tenzir::plugins::all

TENZIR_REGISTER_PLUGIN(tenzir::plugins::all::plugin)
