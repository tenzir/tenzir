//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/aggregation_function.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::sample {

namespace {

class sample_function final : public aggregation_function {
public:
  explicit sample_function(type input_type) noexcept
    : aggregation_function(std::move(input_type)) {
    // nop
  }

private:
  [[nodiscard]] type output_type() const override {
    return input_type();
  }

  void add(const data_view& view) override {
    if (!caf::holds_alternative<caf::none_t>(sample_)) {
      return;
    }
    sample_ = materialize(view);
  }

  void add(const arrow::Array& array) override {
    if (!caf::holds_alternative<caf::none_t>(sample_)) {
      return;
    }
    for (const auto& value : values(input_type(), array)) {
      if (!caf::holds_alternative<caf::none_t>(value)) {
        sample_ = materialize(value);
        return;
      }
    }
  }

  [[nodiscard]] caf::expected<data> finish() && override {
    return std::move(sample_);
  }

  data sample_ = {};
};

class sample_instance final : public aggregation_instance {
public:
  explicit sample_instance(ast::expression expr) : expr_{std::move(expr)} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    if (not caf::holds_alternative<caf::none_t>(sample_)) {
      return;
    }
    auto arg = eval(expr_, input, ctx);
    if (caf::holds_alternative<null_type>(arg.type)) {
      return;
    }
    for (int64_t i = 0; i < arg.array->length(); ++i) {
      if (arg.array->IsValid(i)) {
        sample_ = materialize(value_at(arg.type, *arg.array, i));
        return;
      }
    }
  }

  auto finish() -> data override {
    return sample_;
  }

private:
  ast::expression expr_;
  data sample_;
};

class plugin : public virtual aggregation_function_plugin,
               public virtual aggregation_plugin {
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "sample";
  };

  [[nodiscard]] caf::expected<std::unique_ptr<aggregation_function>>
  make_aggregation_function(const type& input_type) const override {
    return std::make_unique<sample_function>(input_type);
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("tql2.sample")
          .add(expr, "<expr>")
          .parse(inv, ctx));
    return std::make_unique<sample_instance>(std::move(expr));
  }

  auto aggregation_default() const -> data override {
    return caf::none;
  }
};

} // namespace

} // namespace tenzir::plugins::sample

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sample::plugin)
