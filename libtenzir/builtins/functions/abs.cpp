//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/multi_series.hpp"

#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/tenzir/time.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/compute/api.h>
#include <arrow/type_fwd.h>

namespace tenzir::plugins::abs {

namespace {

class abs final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "abs";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "duration|number")
          .parse(inv, ctx));
    return function_use::make([expr = std::move(expr)](
                                evaluator eval, session ctx) -> multi_series {
      const auto evaluated = eval(expr);
      return map_series(evaluated, [&](series arg) {
        return match(
          *arg.array,
          [&](const arrow::NullArray&) -> series {
            return arg;
          },
          [&](const arrow::UInt64Array&) -> series {
            return arg;
          },
          [&](const arrow::Int64Array& array) -> series {
            // TODO: Maybe slice the array for positive values
            auto overflow = false;
            auto b = int64_type::make_arrow_builder(arrow_memory_pool());
            for (auto v : values(int64_type{}, array)) {
              if (not v) {
                check(b->AppendNull());
                continue;
              }
              const auto val = v.value();
              if (val == std::numeric_limits<int64_t>::lowest()) {
                check(b->AppendNull());
                overflow = true;
                continue;
              }
              check(b->Append(std::abs(val)));
            }
            if (overflow) {
              diagnostic::warning("integer overflow").primary(expr).emit(ctx);
            }
            return series{int64_type{}, finish(*b)};
          },
          [&](const arrow::DoubleArray& array) -> series {
            // TODO: Maybe slice the array for positive values
            auto b = double_type::make_arrow_builder(arrow_memory_pool());
            for (auto v : values(double_type{}, array)) {
              if (not v) {
                check(b->AppendNull());
                continue;
              }
              check(b->Append(std::abs(v.value())));
            }
            return series{double_type{}, finish(*b)};
          },
          [&](const arrow::DurationArray& array) {
            // TODO: Maybe slice the array for positive values
            auto overflow = false;
            auto b = duration_type::make_arrow_builder(arrow_memory_pool());
            for (auto v : values(duration_type{}, array)) {
              if (not v) {
                check(b->AppendNull());
                continue;
              }
              const auto val = v->count();
              if (val == std::numeric_limits<duration::rep>::lowest()) {
                check(b->AppendNull());
                overflow = true;
                continue;
              }
              check(b->Append(std::abs(val)));
            }
            if (overflow) {
              diagnostic::warning("duration overflow").primary(expr).emit(ctx);
            }
            return series{duration_type{}, finish(*b)};
          },
          [&](const auto&) {
            diagnostic::warning("expected `duration|number`, but got `{}`",
                                arg.type.kind())
              .primary(expr)
              .emit(ctx);
            return series::null(null_type{}, arg.length());
          });
      });
    });
  }
};

} // namespace

} // namespace tenzir::plugins::abs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::abs::abs)
