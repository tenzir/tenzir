//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::first {

namespace {

class first_instance final : public aggregation_instance {
public:
  explicit first_instance(ast::expression expr) : expr_{std::move(expr)} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    if (not caf::holds_alternative<caf::none_t>(first_)) {
      return;
    }
    auto arg = eval(expr_, input, ctx);
    if (caf::holds_alternative<null_type>(arg.type)) {
      return;
    }
    for (int64_t i = 0; i < arg.array->length(); ++i) {
      if (arg.array->IsValid(i)) {
        first_ = materialize(value_at(arg.type, *arg.array, i));
        return;
      }
    }
  }

  auto finish() -> data override {
    return first_;
  }

private:
  ast::expression expr_ = {};
  data first_ = {};
};

class plugin : public virtual aggregation_plugin {
  auto name() const -> std::string override {
    return "first";
  };

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name()).add(expr, "<expr>").parse(inv, ctx));
    return std::make_unique<first_instance>(std::move(expr));
  }
};

} // namespace

} // namespace tenzir::plugins::first

TENZIR_REGISTER_PLUGIN(tenzir::plugins::first::plugin)
