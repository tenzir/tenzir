//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/parser.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/resolve.hpp>

namespace tenzir::plugins::over {

namespace {

class over_instance : public aggregation_instance {
public:
  over_instance(ast::expression expr, ast::function_call call)
    : expr_{std::move(expr)}, call_{std::move(call)} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    const auto key_series = eval(expr_, input, ctx);
    if (caf::holds_alternative<null_type>(key_series.type)) {
      return;
    }
    const auto keys = key_series.as<string_type>();
    if (not keys) {
      diagnostic::warning("expected `string`, but got `{}`",
                          key_series.type.kind())
        .primary(expr_)
        .emit(ctx);
      return;
    }
    auto previous_key = std::optional<std::string>{};
    const auto update = [&](int64_t begin, int64_t end) {
      if (begin == end or not previous_key) {
        return;
      }
      auto sub = subslice(input, begin, end);
      auto it = instances_.find(*previous_key);
      if (it == instances_.end()) {
        const auto* fn
          = dynamic_cast<const aggregation_plugin*>(&ctx.reg().get(call_));
        TENZIR_ASSERT(fn);
        auto instance
          = fn->make_aggregation(aggregation_plugin::invocation{call_}, ctx);
        TENZIR_ASSERT(instance);
        it = instances_.emplace_hint(it, std::string{*previous_key},
                                     std::move(*instance));
      }
      it->second->update(subslice(input, begin, end), ctx);
    };
    auto first = int64_t{0};
    auto last = int64_t{0};
    while (last < keys->length()) {
      const auto is_null = keys->array->IsNull(last);
      // If the key is null, we must flush.
      if (is_null) {
        update(first, last);
        ++last;
        first = last;
        previous_key.reset();
        continue;
      }
      // If the key stayed the same, we can keep going.
      const auto view = keys->array->GetView(last);
      if (view == previous_key) {
        ++last;
        continue;
      }
      // If the key changed and we have a previous key, we must flush.
      if (previous_key) {
        update(first, last);
      }
      // Update the key for the next round.
      first = last;
      ++last;
      previous_key = std::string{view};
    }
    update(first, last);
  }

  auto finish() -> data override {
    auto result = record{};
    result.reserve(instances_.size());
    for (const auto& [key, instance] : instances_) {
      result.emplace(key, instance->finish());
    }
    std::ranges::sort(result, std::less<>{}, [](const auto& x) {
      return x.first;
    });
    return result;
  }

private:
  ast::expression expr_ = {};
  ast::function_call call_ = {};
  tsl::robin_map<std::string, std::unique_ptr<aggregation_instance>> instances_
    = {};
};

class plugin : public virtual aggregation_plugin {
  auto name() const -> std::string override {
    return "over";
  };

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    // value.sum().over(key)
    auto call = ast::expression{};
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .add(call, "<aggregation>")
          .add(expr, "<expr>")
          .parse(inv, ctx));
    return call.match(
      [&](ast::function_call& call)
        -> failure_or<std::unique_ptr<aggregation_instance>> {
        const auto* fn
          = dynamic_cast<const aggregation_plugin*>(&ctx.reg().get(call));
        if (not fn) {
          diagnostic::error("function does not support aggregations")
            .primary(call.fn)
            .emit(ctx);
          return failure::promise();
        }
        if (not fn->make_aggregation(aggregation_plugin::invocation{call},
                                     ctx)) {
          return failure::promise();
        }
        return std::make_unique<over_instance>(std::move(expr),
                                               std::move(call));
      },
      [&](const auto&) -> failure_or<std::unique_ptr<aggregation_instance>> {
        diagnostic::error("expected aggregation function call")
          .primary(call)
          .emit(ctx);
        return failure::promise();
      });
  }
};

} // namespace

} // namespace tenzir::plugins::over

TENZIR_REGISTER_PLUGIN(tenzir::plugins::over::plugin)
