//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_time_utils.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/tenzir/si.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/util/tdigest.h>

namespace tenzir::plugins::numeric {
namespace {

template <bool Ceil>
class plugin final : public function_plugin {
public:
  auto name() const -> std::string override {
    return Ceil ? "ceil" : "floor";
  }

  auto make_function(invocation inv,
                     session ctx) const -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto spec = std::optional<located<duration>>{};
    TRY(argument_parser2::function(name())
          .add(expr, "<value>")
          .add(spec, "<spec>")
          .parse(inv, ctx));
    if (spec && spec->inner.count() == 0) {
      diagnostic::error("resolution must not be 0")
        .primary(spec.value())
        .emit(ctx);
      return failure::promise();
    }
    return function_use::make([expr = std::move(expr), spec = std::move(spec),
                               this](evaluator eval, session ctx) -> series {
      const auto value = located{eval(expr), expr.get_location()};
      const auto& ty = value.inner.type;
      const auto length = value.inner.length();
      if (not spec) {
        // fn(<number>)
        const auto f = detail::overload{
          [&](const arrow::NullArray&) {
            return series::null(ty, length);
          },
          [&]<concepts::one_of<arrow::Int64Array, arrow::UInt64Array> T>(
            const T&) {
            return value.inner;
          },
          [&](const arrow::DoubleArray& arg) {
            auto b = arrow::DoubleBuilder{};
            check(b.Reserve(length));
            for (auto row = int64_t{0}; row < length; ++row) {
              if (arg.IsNull(row) or not std::isfinite(arg.Value(row))) {
                check(b.AppendNull());
                continue;
              }
              if constexpr (Ceil) {
                check(b.Append(std::ceil(arg.Value(row))));
              } else {
                check(b.Append(std::floor(arg.Value(row))));
              }
            }
            return series{ty, finish(b)};
          },
          [&]<concepts::one_of<arrow::DurationArray, arrow::TimestampArray> T>(
            const T&) {
            diagnostic::warning("`{}` with duration requires second argument",
                                name())
              .primary(value)
              .hint("for example `{}(x, 1h)`", name())
              .emit(ctx);
            return series::null(ty, length);
          },
          [&](const auto&) {
            diagnostic::warning("`{}` expected `number`, got `{}`", name(),
                                ty.kind())
              // TODO: Wrong location.
              .primary(value)
              .emit(ctx);
            return series::null(ty, length);
          },
        };
        return caf::visit(f, *value.inner.array);
      }
      // fn(<duration>, <duration>)
      // fn(x, 1h) -> to multiples of 1h
      // fn(<time>, <duration>)
      // fn(x, 1h) -> time is multiples of 1h (for UTC timezone?)
      const auto f = detail::overload{
        [&](const arrow::DurationArray& array) -> series {
          auto b
            = duration_type::make_arrow_builder(arrow::default_memory_pool());
          check(b->Reserve(array.length()));
          for (auto i = int64_t{0}; i < array.length(); i++) {
            if (array.IsNull(i)) {
              check(b->AppendNull());
              continue;
            }
            const auto val = array.Value(i);
            const auto count = std::abs(spec->inner.count());
            const auto rem = val % count;
            if constexpr (Ceil) {
              check(b->Append(val + (count - rem)));
            } else {
              check(b->Append(val - rem));
            }
          }
          return {duration_type{}, finish(*b)};
        },
        [&](const arrow::TimestampArray& array) -> series {
          auto opts = make_round_temporal_options(spec->inner);
          if constexpr (Ceil) {
            return {time_type{},
                    check(arrow::compute::CeilTemporal(array, std::move(opts)))
                      .array_as<arrow::TimestampArray>()};
          } else {
            return {time_type{},
                    check(arrow::compute::FloorTemporal(array, std::move(opts)))
                      .array_as<arrow::TimestampArray>()};
          }
        },
        [&](const auto&) {
          diagnostic::warning("{}(_, _) is not implemented for {}", name(), ty)
            .primary(value)
            .emit(ctx);
          return series::null(ty, length);
        }};
      return caf::visit(f, *value.inner.array);
    });
  }
};

} // namespace
} // namespace tenzir::plugins::numeric

TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::plugin<false>)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::plugin<true>)
