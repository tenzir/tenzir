//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/detail/geodesic.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <algorithm>
#include <array>
#include <cmath>
#include <numbers>

namespace tenzir::plugins::geo_distance {

namespace {

constexpr auto mean_earth_radius = 6'371'008.8;

auto is_number_or_null(type const& type) -> bool {
  return is<int64_type>(type) or is<uint64_type>(type) or is<double_type>(type)
         or is<null_type>(type);
}

auto number_at(series const& input, int64_t row) -> Option<double> {
  return match(
    *input.array,
    [&](arrow::NullArray const&) -> Option<double> {
      return None{};
    },
    [&]<
      concepts::one_of<arrow::Int64Array, arrow::UInt64Array, arrow::DoubleArray>
        Array>(Array const& array) -> Option<double> {
      if (array.IsNull(row)) {
        return None{};
      }
      return static_cast<double>(array.Value(row));
    },
    [](auto const&) -> Option<double> {
      TENZIR_UNREACHABLE();
    });
}

auto bool_at(series const& input, int64_t row) -> Option<bool> {
  return match(
    *input.array,
    [&](arrow::NullArray const&) -> Option<bool> {
      return None{};
    },
    [&](arrow::BooleanArray const& array) -> Option<bool> {
      if (array.IsNull(row)) {
        return None{};
      }
      return array.Value(row);
    },
    [](auto const&) -> Option<bool> {
      TENZIR_UNREACHABLE();
    });
}

auto central_angle(double lon1, double lat1, double lon2, double lat2)
  -> double {
  constexpr auto degrees_to_radians = std::numbers::pi / 180.0;
  auto phi1 = lat1 * degrees_to_radians;
  auto phi2 = lat2 * degrees_to_radians;
  auto delta_phi = (lat2 - lat1) * degrees_to_radians;
  auto delta_lambda = (lon2 - lon1) * degrees_to_radians;
  auto sin_delta_phi = std::sin(delta_phi / 2.0);
  auto sin_delta_lambda = std::sin(delta_lambda / 2.0);
  auto haversine
    = sin_delta_phi * sin_delta_phi
      + std::cos(phi1) * std::cos(phi2) * sin_delta_lambda * sin_delta_lambda;
  return 2.0 * std::asin(std::sqrt(std::clamp(haversine, 0.0, 1.0)));
}

auto spherical_distance(double lon1, double lat1, double lon2, double lat2)
  -> double {
  return mean_earth_radius * central_angle(lon1, lat1, lon2, lat2);
}

auto spheroidal_distance(double lon1, double lat1, double lon2, double lat2)
  -> double {
  return tenzir::detail::wgs84_distance(lon1, lat1, lon2, lat2);
}

class plugin final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.geo_distance";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto lon1 = ast::expression{};
    auto lat1 = ast::expression{};
    auto lon2 = ast::expression{};
    auto lat2 = ast::expression{};
    auto spheroid = Option<ast::expression>{};
    TRY(argument_parser2::function("geo_distance")
          .positional("lon1", lon1, "number")
          .positional("lat1", lat1, "number")
          .positional("lon2", lon2, "number")
          .positional("lat2", lat2, "number")
          .named("spheroid", spheroid, "bool")
          .parse(inv, ctx));
    return function_use::make(
      [lon1 = std::move(lon1), lat1 = std::move(lat1), lon2 = std::move(lon2),
       lat2 = std::move(lat2),
       spheroid = std::move(spheroid)](evaluator eval, session ctx) {
        auto inputs = std::vector<multi_series>{};
        inputs.reserve(spheroid ? 5 : 4);
        inputs.push_back(eval(lon1));
        inputs.push_back(eval(lat1));
        inputs.push_back(eval(lon2));
        inputs.push_back(eval(lat2));
        if (spheroid) {
          inputs.push_back(eval(*spheroid));
        }
        auto expressions = std::array<ast::expression const*, 4>{
          &lon1,
          &lat1,
          &lon2,
          &lat2,
        };
        auto names = std::array<std::string_view, 4>{
          "lon1",
          "lat1",
          "lon2",
          "lat2",
        };
        return map_series(
          std::span<const multi_series>{inputs},
          [&](std::span<series> parts) -> multi_series {
            auto builder = arrow::DoubleBuilder{arrow_memory_pool()};
            check(builder.Reserve(parts.front().length()));
            for (auto index = size_t{0}; index < 4; ++index) {
              if (is_number_or_null(parts[index].type)) {
                continue;
              }
              diagnostic::warning("`geo_distance` expected `{}` to be "
                                  "`number`, but got `{}`",
                                  names[index], parts[index].type.kind())
                .primary(*expressions[index])
                .emit(ctx);
              check(builder.AppendNulls(parts.front().length()));
              return series{double_type{}, finish(builder)};
            }
            if (parts.size() == 5 and not is<bool_type>(parts[4].type)
                and not is<null_type>(parts[4].type)) {
              diagnostic::warning("`geo_distance` expected `spheroid` to be "
                                  "`bool`, but got "
                                  "`{}`",
                                  parts[4].type.kind())
                .primary(*spheroid)
                .emit(ctx);
              check(builder.AppendNulls(parts.front().length()));
              return series{double_type{}, finish(builder)};
            }
            for (auto row = int64_t{0}; row < parts.front().length(); ++row) {
              auto longitude1 = number_at(parts[0], row);
              auto latitude1 = number_at(parts[1], row);
              auto longitude2 = number_at(parts[2], row);
              auto latitude2 = number_at(parts[3], row);
              auto use_spheroid = parts.size() == 5 ? bool_at(parts[4], row)
                                                    : Option<bool>{false};
              if (not longitude1 or not latitude1 or not longitude2
                  or not latitude2 or not use_spheroid
                  or not std::isfinite(*longitude1)
                  or not std::isfinite(*latitude1)
                  or not std::isfinite(*longitude2)
                  or not std::isfinite(*latitude2) or *longitude1 < -180.0
                  or *longitude1 > 180.0 or *latitude1 < -90.0
                  or *latitude1 > 90.0 or *longitude2 < -180.0
                  or *longitude2 > 180.0 or *latitude2 < -90.0
                  or *latitude2 > 90.0) {
                check(builder.AppendNull());
                continue;
              }
              auto distance = *use_spheroid
                                ? spheroidal_distance(*longitude1, *latitude1,
                                                      *longitude2, *latitude2)
                                : spherical_distance(*longitude1, *latitude1,
                                                     *longitude2, *latitude2);
              if (not std::isfinite(distance) or distance < 0.0) {
                check(builder.AppendNull());
                continue;
              }
              check(builder.Append(distance));
            }
            return series{double_type{}, finish(builder)};
          });
      });
  }
};

} // namespace

} // namespace tenzir::plugins::geo_distance

TENZIR_REGISTER_PLUGIN(tenzir::plugins::geo_distance::plugin)
