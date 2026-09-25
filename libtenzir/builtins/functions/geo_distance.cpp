//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/concepts.hpp>
#include <tenzir/detail/geodesic.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/eval_kernel.hpp>
#include <tenzir/nova/function_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <algorithm>
#include <array>
#include <cmath>
#include <numbers>
#include <vector>

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

/// Computes the distance in meters, or `None` for invalid coordinates.
auto distance(double lon1, double lat1, double lon2, double lat2, bool spheroid)
  -> Option<double> {
  auto valid_longitude = [](double x) {
    return std::isfinite(x) and x >= -180.0 and x <= 180.0;
  };
  auto valid_latitude = [](double x) {
    return std::isfinite(x) and x >= -90.0 and x <= 90.0;
  };
  if (not valid_longitude(lon1) or not valid_latitude(lat1)
      or not valid_longitude(lon2) or not valid_latitude(lat2)) {
    return None{};
  }
  auto result = spheroid ? spheroidal_distance(lon1, lat1, lon2, lat2)
                         : spherical_distance(lon1, lat1, lon2, lat2);
  if (not std::isfinite(result) or result < 0.0) {
    return None{};
  }
  return result;
}

/// Extracts the values of `arg` at the rows of `mask` as `Out`, leaving null
/// and mistyped rows empty. `Accepts` decides which alternatives are valid;
/// the first mistyped alternative with active rows emits a warning.
template <class Out, class Accepts>
auto extract(nova::ValueArgument const& arg, nova::storage::BitMap const& mask,
             std::string_view expected, diagnostic_handler& dh)
  -> std::vector<Option<Out>> {
  auto result = std::vector<Option<Out>>(static_cast<size_t>(mask.length()));
  auto warned = false;
  auto visit = [&]<nova::data_type Tag>(nova::Array<Tag> const& array,
                                        nova::storage::BitMap const& rows) {
    if constexpr (Accepts::template value<Tag>) {
      nova::storage::for_each_true(rows, [&](nova::storage::Index i) {
        result[static_cast<size_t>(i)] = static_cast<Out>(*array.get(i));
      });
    } else if constexpr (not std::same_as<Tag, nova::Null>) {
      if (not warned and rows.any()) {
        warned = true;
        diagnostic::warning("`geo_distance` expected `{}`, but got `{}`",
                            expected, nova::Type<Tag>::static_name)
          .primary(arg.source)
          .emit(dh);
      }
    }
  };
  match(
    arg.data,
    [&]<nova::data_type Tag>(nova::Array<Tag> const& array) {
      visit(array, mask);
    },
    [&](nova::UnionArray const& array) {
      for (auto const& field : array.fields()) {
        match(field.data,
              [&]<nova::data_type Tag>(nova::Array<Tag> const& alternative) {
                visit(alternative, mask & field.present);
              });
      }
    });
  return result;
}

struct AcceptsNumber {
  template <class Tag>
  static constexpr bool value
    = concepts::one_of<Tag, nova::Int, nova::UInt, nova::Float>;
};

struct AcceptsBool {
  template <class Tag>
  static constexpr bool value = std::same_as<Tag, nova::Bool>;
};

struct GeoDistanceArgs {
  nova::ValueArgument lon1;
  nova::ValueArgument lat1;
  nova::ValueArgument lon2;
  nova::ValueArgument lat2;
  Option<nova::ValueArgument> spheroid;
};

struct GeoDistanceFunction {
  static auto eval(GeoDistanceArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto const& mask = frame.mask();
    auto number = [&](nova::ValueArgument const& arg) {
      return extract<double, AcceptsNumber>(arg, mask, "number", frame);
    };
    auto lon1 = number(args.lon1);
    auto lat1 = number(args.lat1);
    auto lon2 = number(args.lon2);
    auto lat2 = number(args.lat2);
    auto spheroid = std::vector<Option<bool>>{};
    if (args.spheroid) {
      spheroid
        = extract<bool, AcceptsBool>(*args.spheroid, mask, "bool", frame);
    }
    auto results = nova::Results{mask.length()};
    nova::storage::for_each_true(mask, [&](nova::storage::Index row) {
      auto i = static_cast<size_t>(row);
      auto use_spheroid = args.spheroid ? spheroid[i] : Option<bool>{false};
      if (not lon1[i] or not lat1[i] or not lon2[i] or not lat2[i]
          or not use_spheroid) {
        results.set_null(row);
        return;
      }
      auto result
        = distance(*lon1[i], *lat1[i], *lon2[i], *lat2[i], *use_spheroid);
      if (not result) {
        results.set_null(row);
        return;
      }
      results.set<nova::Float>(row, *result);
    });
    return std::move(results).finish(mask);
  }
};

class plugin final : public nova::FunctionPlugin {
public:
  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<GeoDistanceArgs, GeoDistanceFunction>{};
    d.positional("lon1", &GeoDistanceArgs::lon1, "number");
    d.positional("lat1", &GeoDistanceArgs::lat1, "number");
    d.positional("lon2", &GeoDistanceArgs::lon2, "number");
    d.positional("lat2", &GeoDistanceArgs::lat2, "number");
    d.named_optional("spheroid", &GeoDistanceArgs::spheroid, "bool");
    return std::move(d).finish();
  }

  auto name() const -> std::string override {
    return "geo_distance";
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
