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

namespace tenzir::plugins::mode {

namespace {

class mode_instance final : public aggregation_instance {
public:
  explicit mode_instance(ast::expression expr) : expr_{std::move(expr)} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    auto arg = eval(expr_, input, ctx);
    if (caf::holds_alternative<null_type>(arg.type)) {
      return;
    }
    for (int64_t i = 0; i < arg.array->length(); ++i) {
      if (arg.array->IsValid(i)) {
        const auto& view = value_at(arg.type, *arg.array, i);
        auto it = counts_.find(view);
        if (it == counts_.end()) {
          counts_.emplace_hint(it, materialize(view), 1);
          continue;
        }
        ++it.value();
        return;
      }
    }
  }

  auto finish() -> data override {
    const auto comp = [](const auto& lhs, const auto& rhs) {
      return lhs.second < rhs.second;
    };
    const auto it = std::ranges::max_element(counts_, comp);
    if (it == counts_.end()) {
      return {};
    }
    return it->first;
  }

private:
  const ast::expression expr_ = {};
  tsl::robin_map<data, int64_t> counts_ = {};
};

class plugin : public virtual aggregation_plugin {
  auto name() const -> std::string override {
    return "mode";
  };

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name()).add(expr, "<expr>").parse(inv, ctx));
    return std::make_unique<mode_instance>(std::move(expr));
  }
};

} // namespace

} // namespace tenzir::plugins::mode

TENZIR_REGISTER_PLUGIN(tenzir::plugins::mode::plugin)
