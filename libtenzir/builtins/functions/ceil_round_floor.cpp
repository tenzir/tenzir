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

TENZIR_ENUM(mode, ceil, floor, round);

namespace {
template <mode Mode>
class plugin final : public function_plugin {
public:
  auto name() const -> std::string override {
    return std::string{to_string(Mode)};
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
                               inv_loc = inv.call.get_location(),
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
            // overflow logic from int.cpp
            auto b = arrow::Int64Builder{};
            check(b.Reserve(length));
            constexpr auto min
              = static_cast<double>(std::numeric_limits<int64_t>::lowest())
                - 1.0;
            constexpr auto max
              = static_cast<double>(std::numeric_limits<int64_t>::max()) + 1.0;
            auto overflow = false;
            for (auto row = int64_t{0}; row < length; ++row) {
              if (arg.IsNull(row) or not std::isfinite(arg.Value(row))) {
                check(b.AppendNull());
                continue;
              }
              auto val = [&]() {
                if constexpr (Mode == mode::ceil) {
                  return std::ceil(arg.Value(row));
                } else if constexpr (Mode == mode::floor) {
                  return std::floor(arg.Value(row));
                } else {
                  TENZIR_ASSERT(Mode == mode::round);
                  return std::round(arg.Value(row));
                }
              }();
              if (not(val > min) || not(val < max)) {
                check(b.AppendNull());
                overflow = true;
                continue;
              }
              check(b.Append(static_cast<int64_t>(val)));
            }
            if (overflow) {
              diagnostic::warning("integer overflow in `{}`", name())
                .primary(expr)
                .emit(ctx);
            }
            return series{int64_type{}, finish(b)};
          },
          [&]<concepts::one_of<arrow::DurationArray, arrow::TimestampArray> T>(
            const T&) {
            diagnostic::warning("`{}` with `{}` requires a resolution", name(),
                                ty.kind())
              .primary(value)
              .hint("for example `{}(x, 1h)`", name())
              .emit(ctx);
            return series::null(ty, length);
          },
          [&](const auto&) {
            diagnostic::warning("`{}` expected `number`, got `{}`", name(),
                                ty.kind())
              .primary(value)
              .emit(ctx);
            return series::null(ty, length);
          },
        };
        return match(*value.inner.array, f);
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
            const auto rem = std::abs(val % count);
            if (rem == 0) {
              check(b->Append(val));
              continue;
            }
            const auto ceil = val >= 0 ? count - rem : rem;
            const auto floor = val >= 0 ? -rem : rem - count;
            if constexpr (Mode == mode::ceil) {
              check(b->Append(val + ceil));
            } else if constexpr (Mode == mode::floor) {
              check(b->Append(val + floor));
            } else {
              check(b->Append(val + (std::abs(floor) < ceil ? floor : ceil)));
            }
          }
          return {duration_type{}, finish(*b)};
        },
        [&](const arrow::TimestampArray& array) -> series {
          auto opts = make_round_temporal_options(spec->inner);
          if constexpr (Mode == mode::ceil) {
            return {time_type{},
                    check(arrow::compute::CeilTemporal(array, std::move(opts)))
                      .array_as<arrow::TimestampArray>()};
          } else if constexpr (Mode == mode::floor) {
            return {time_type{},
                    check(arrow::compute::FloorTemporal(array, std::move(opts)))
                      .array_as<arrow::TimestampArray>()};
          } else {
            return {time_type{},
                    check(arrow::compute::RoundTemporal(array, std::move(opts)))
                      .array_as<arrow::TimestampArray>()};
          }
        },
        [&](const auto&) {
          diagnostic::warning(
            "`{}(value, resolution)` is not implemented for {}", name(), ty)
            .primary(inv_loc)
            .emit(ctx);
          return series::null(ty, length);
        }};
      return match(*value.inner.array, f);
    });
  }
};

using ceil_plugin = plugin<mode::ceil>;
using floor_plugin = plugin<mode::floor>;
using round_plugin = plugin<mode::round>;

} // namespace
} // namespace tenzir::plugins::numeric

TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::ceil_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::floor_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::round_plugin)
