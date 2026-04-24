//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/tenzir/ip.hpp>
#include <tenzir/concept/parseable/tenzir/subnet.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/type_traits.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/subnet.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/type.hpp>
#include <tenzir/view3.hpp>

#include <string_view>
#include <tuple>
#include <type_traits>

namespace tenzir::plugins::subnet {

namespace {

auto to_internal_prefix(tenzir::ip const& value, uint64_t prefix)
  -> Option<uint8_t> {
  if (value.is_v4()) {
    if (prefix > 32) {
      return None{};
    }
    return detail::narrow<uint8_t>(prefix + 96);
  }
  if (prefix > 128) {
    return None{};
  }
  return detail::narrow<uint8_t>(prefix);
}

auto make_subnet(tenzir::ip const& value) -> tenzir::subnet {
  return {value, 128};
}

auto make_subnet(tenzir::ip const& value, uint64_t prefix)
  -> Option<tenzir::subnet> {
  auto length = to_internal_prefix(value, prefix);
  if (not length) {
    return None{};
  }
  return tenzir::subnet{value, *length};
}

auto make_subnet(tenzir::ip const& value, int64_t prefix)
  -> Option<tenzir::subnet> {
  if (prefix < 0) {
    return None{};
  }
  return make_subnet(value, detail::narrow<uint64_t>(prefix));
}

auto parse_subnet(std::string_view value) -> Option<tenzir::subnet> {
  auto result = tenzir::subnet{};
  if (parsers::net(value, result)) {
    return result;
  }
  auto ip = tenzir::ip{};
  if (parsers::ip(value, ip)) {
    return make_subnet(ip);
  }
  return None{};
}

template <class Prefix>
auto parse_subnet(std::string_view value, Prefix prefix)
  -> Option<tenzir::subnet> {
  auto ip = tenzir::ip{};
  if (not parsers::ip(value, ip)) {
    return None{};
  }
  return make_subnet(ip, prefix);
}

auto append_subnet(subnet_type::builder_type& builder,
                   Option<tenzir::subnet> value) -> void {
  if (not value) {
    check(builder.AppendNull());
    return;
  }
  check(append_builder(subnet_type{}, builder, *value));
}

auto append_without_prefix(subnet_type::builder_type& builder,
                           series const& value, ast::expression const& expr,
                           session ctx) -> void {
  auto f = detail::overload{
    [&](const arrow::NullArray& array) {
      check(builder.AppendNulls(array.length()));
    },
    [&](const arrow::StringArray& array) {
      for (auto i = int64_t{0}; i < array.length(); ++i) {
        if (array.IsNull(i)) {
          check(builder.AppendNull());
          continue;
        }
        append_subnet(builder, parse_subnet(array.GetView(i)));
      }
    },
    [&](const ip_type::array_type& array) {
      for (auto i = int64_t{0}; i < array.length(); ++i) {
        if (auto ip = view_at(array, i)) {
          check(append_builder(subnet_type{}, builder, make_subnet(*ip)));
        } else {
          check(builder.AppendNull());
        }
      }
    },
    [&](const subnet_type::array_type& array) {
      check(append_array(builder, subnet_type{}, array));
    },
    [&](const auto& array) {
      diagnostic::warning("`subnet` expected `string`, `ip`, or `subnet`, but "
                          "got `{}`",
                          value.type.kind())
        .primary(expr)
        .emit(ctx);
      check(builder.AppendNulls(array.length()));
    },
  };
  match(*value.array, f);
}

template <class PrefixArray>
auto append_with_prefix(subnet_type::builder_type& builder,
                        arrow::StringArray const& values,
                        PrefixArray const& prefixes) -> void {
  for (auto i = int64_t{0}; i < values.length(); ++i) {
    if (values.IsNull(i) or prefixes.IsNull(i)) {
      check(builder.AppendNull());
      continue;
    }
    append_subnet(builder, parse_subnet(values.GetView(i), prefixes.Value(i)));
  }
}

template <class PrefixArray>
auto append_with_prefix(subnet_type::builder_type& builder,
                        ip_type::array_type const& values,
                        PrefixArray const& prefixes) -> void {
  for (auto i = int64_t{0}; i < values.length(); ++i) {
    auto ip = view_at(values, i);
    if (not ip or prefixes.IsNull(i)) {
      check(builder.AppendNull());
      continue;
    }
    append_subnet(builder, make_subnet(*ip, prefixes.Value(i)));
  }
}

auto append_with_prefix(subnet_type::builder_type& builder, series const& value,
                        series const& prefix, ast::expression const& value_expr,
                        ast::expression const& prefix_expr, session ctx)
  -> void {
  auto f = detail::overload{
    [&](concepts::one_of<arrow::StringArray, ip_type::array_type> auto const&
          values,
        concepts::one_of<arrow::Int64Array, arrow::UInt64Array> auto const&
          prefixes) {
      append_with_prefix(builder, values, prefixes);
    },
    [&](const auto& values, const auto& prefixes) {
      using values_type = std::remove_cvref_t<decltype(values)>;
      using prefixes_type = std::remove_cvref_t<decltype(prefixes)>;
      if constexpr (not detail::is_any_v<values_type, arrow::StringArray,
                                         ip_type::array_type, arrow::NullArray>) {
        diagnostic::warning("`subnet` expected `string` or `ip`, but got `{}`",
                            value.type.kind())
          .primary(value_expr)
          .emit(ctx);
      }
      if constexpr (not detail::is_any_v<prefixes_type, arrow::Int64Array,
                                         arrow::UInt64Array, arrow::NullArray>) {
        diagnostic::warning("`subnet` expected `int`, but got `{}`",
                            prefix.type.kind())
          .primary(prefix_expr)
          .emit(ctx);
      }
      check(builder.AppendNulls(values.length()));
    },
  };
  match(std::tie(*value.array, *prefix.array), f);
}

class subnet final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.subnet";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto prefix_expr = Option<ast::expression>{};
    TRY(argument_parser2::function("subnet")
          .positional("x", expr, "string|ip|subnet")
          .positional("prefix", prefix_expr, "int")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr), prefix_expr = std::move(prefix_expr)](
        evaluator eval, session ctx) -> series {
        auto b = subnet_type::make_arrow_builder(arrow_memory_pool());
        check(b->Reserve(eval.length()));
        if (prefix_expr) {
          for (auto [value, prefix] :
               split_multi_series(eval(expr), eval(*prefix_expr))) {
            TENZIR_ASSERT(value.length() == prefix.length());
            append_with_prefix(*b, value, prefix, expr, *prefix_expr, ctx);
          }
        } else {
          for (auto const& value : eval(expr)) {
            append_without_prefix(*b, value, expr, ctx);
          }
        }
        return series{subnet_type{}, check(b->Finish())};
      });
  }
};

} // namespace

} // namespace tenzir::plugins::subnet

TENZIR_REGISTER_PLUGIN(tenzir::plugins::subnet::subnet)
