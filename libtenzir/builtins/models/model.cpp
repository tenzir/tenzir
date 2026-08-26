//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/model.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/view3.hpp>

#include <fmt/format.h>

#include <string>
#include <string_view>

namespace tenzir::plugins::model {

namespace {

auto parse_provider(record_view3 value)
  -> Result<std::pair<model_envelope, model_plugin const*>, std::string> {
  auto envelope_result = parse_model_envelope(value);
  if (not envelope_result) {
    return Err{
      fmt::format("malformed model record: {}", envelope_result.unwrap_err())};
  }
  auto envelope = std::move(envelope_result).unwrap();
  auto provider_result = find_model_plugin(envelope);
  if (not provider_result) {
    return Err{std::move(provider_result).unwrap_err()};
  }
  return std::pair{std::move(envelope), provider_result.unwrap()};
}

auto warn_once(bool& warned, std::string const& message,
               ast::expression const& primary, session ctx) -> void {
  if (warned) {
    return;
  }
  warned = true;
  diagnostic::warning("{}", message)
    .primary(primary.get_location().subloc(0, 1))
    .emit(ctx);
}

enum class comparison_kind { divergence, distance };

template <comparison_kind Kind>
class comparison final : public function_plugin {
public:
  auto name() const -> std::string override {
    if constexpr (Kind == comparison_kind::divergence) {
      return "model_divergence";
    }
    return "model_distance";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto lhs_expr = ast::expression{};
    auto rhs_expr = ast::expression{};
    auto method = located<std::string>{};
    TRY(argument_parser2::function(name())
          .positional("p", lhs_expr, "record")
          .positional("q", rhs_expr, "record")
          .named("method", method)
          .parse(inv, ctx));
    if constexpr (Kind == comparison_kind::divergence) {
      if (method.inner != "jensen_shannon") {
        diagnostic::error("unsupported divergence method `{}`", method.inner)
          .primary(method)
          .hint("use `jensen_shannon`")
          .emit(ctx);
        return failure::promise();
      }
    } else {
      if (method.inner != "kolmogorov_smirnov"
          and method.inner != "wasserstein") {
        diagnostic::error("unsupported distance method `{}`", method.inner)
          .primary(method)
          .hint("use `kolmogorov_smirnov` or `wasserstein`")
          .emit(ctx);
        return failure::promise();
      }
    }
    return function_use::make([lhs_expr = std::move(lhs_expr),
                               rhs_expr = std::move(rhs_expr),
                               method = std::move(method.inner)](
                                evaluator eval, session ctx) -> multi_series {
      return map_series(
        eval(lhs_expr), eval(rhs_expr), [&](series lhs, series rhs) -> series {
          auto builder = double_type::make_arrow_builder(arrow_memory_pool());
          if (is<null_type>(lhs.type) or is<null_type>(rhs.type)) {
            check(builder->AppendNulls(lhs.length()));
            return series{double_type{}, finish(*builder)};
          }
          auto lhs_records = lhs.as<record_type>();
          auto rhs_records = rhs.as<record_type>();
          if (not lhs_records or not rhs_records) {
            auto const& invalid = not lhs_records ? lhs : rhs;
            auto const& expression = not lhs_records ? lhs_expr : rhs_expr;
            diagnostic::warning("expected `record`, got `{}`",
                                invalid.type.kind())
              .primary(expression.get_location().subloc(0, 1))
              .emit(ctx);
            check(builder->AppendNulls(lhs.length()));
            return series{double_type{}, finish(*builder)};
          }
          auto lhs_values = values3(*lhs_records->array);
          auto rhs_values = values3(*rhs_records->array);
          auto lhs_it = lhs_values.begin();
          auto rhs_it = rhs_values.begin();
          auto warned = false;
          for (; lhs_it != lhs_values.end(); ++lhs_it, ++rhs_it) {
            if (not *lhs_it or not *rhs_it) {
              check(builder->AppendNull());
              continue;
            }
            auto lhs_provider = parse_provider(**lhs_it);
            if (not lhs_provider) {
              warn_once(warned, lhs_provider.unwrap_err(), lhs_expr, ctx);
              check(builder->AppendNull());
              continue;
            }
            auto rhs_envelope = parse_model_envelope(**rhs_it);
            if (not rhs_envelope) {
              warn_once(warned,
                        fmt::format("malformed model record: {}",
                                    rhs_envelope.unwrap_err()),
                        rhs_expr, ctx);
              check(builder->AppendNull());
              continue;
            }
            auto lhs_envelope = std::move(lhs_provider).unwrap().first;
            auto rhs_model_envelope = std::move(rhs_envelope).unwrap();
            if (lhs_envelope.model != rhs_model_envelope.model
                or lhs_envelope.version != rhs_model_envelope.version) {
              warn_once(warned,
                        fmt::format("cannot compare `{}` version {} with `{}` "
                                    "version {}",
                                    lhs_envelope.model, lhs_envelope.version,
                                    rhs_model_envelope.model,
                                    rhs_model_envelope.version),
                        lhs_expr, ctx);
              check(builder->AppendNull());
              continue;
            }
            auto const* plugin = [&] {
              if constexpr (Kind == comparison_kind::divergence) {
                return plugins::find<model_divergence_plugin>(
                  lhs_envelope.model);
              } else {
                return plugins::find<model_distance_plugin>(lhs_envelope.model);
              }
            }();
            if (not plugin) {
              auto const operation = Kind == comparison_kind::divergence
                                       ? "divergence"
                                       : "distance";
              warn_once(warned,
                        fmt::format("model `{}` does not support {} method "
                                    "`{}`",
                                    lhs_envelope.model, operation, method),
                        lhs_expr, ctx);
              check(builder->AppendNull());
              continue;
            }
            auto result = [&] {
              if constexpr (Kind == comparison_kind::divergence) {
                return plugin->model_divergence(**lhs_it, **rhs_it, method);
              } else {
                return plugin->model_distance(**lhs_it, **rhs_it, method);
              }
            }();
            if (not result) {
              warn_once(warned, result.unwrap_err(), lhs_expr, ctx);
              check(builder->AppendNull());
              continue;
            }
            auto value = std::move(result).unwrap();
            if (not value) {
              check(builder->AppendNull());
              continue;
            }
            check(builder->Append(*value));
          }
          return series{double_type{}, finish(*builder)};
        });
    });
  }
};

using divergence = comparison<comparison_kind::divergence>;
using distance = comparison<comparison_kind::distance>;

} // namespace

} // namespace tenzir::plugins::model

TENZIR_REGISTER_PLUGIN(tenzir::plugins::model::divergence)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::model::distance)
