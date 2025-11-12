//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/tenzir/ip.hpp>
#include <tenzir/concept/parseable/tenzir/time.hpp>
#include <tenzir/ip.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/type.hpp>

namespace tenzir::plugins::ip {

namespace {

class ip final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.ip";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("ip")
          .positional("x", expr, "string")
          .parse(inv, ctx));
    return function_use::make([expr
                               = std::move(expr)](evaluator eval, session ctx) {
      return map_series(eval(expr), [&](series arg) {
        auto f = detail::overload{
          [](const arrow::NullArray& arg) {
            return series::null(ip_type{}, arg.length());
          },
          [](const arrow::StringArray& arg) {
            auto b = ip_type::make_arrow_builder(arrow_memory_pool());
            check(b->Reserve(arg.length()));
            for (auto i = 0; i < arg.length(); ++i) {
              if (arg.IsNull(i)) {
                check(b->AppendNull());
                continue;
              }
              auto result = tenzir::ip{};
              if (parsers::ip(arg.GetView(i), result)) {
                check(append_builder(ip_type{}, *b, result));
              } else {
                // TODO: ?
                check(b->AppendNull());
              }
            }
            return series{ip_type{}, check(b->Finish())};
          },
          [&](const ip_type::array_type&) {
            return arg;
          },
          [&](const auto&) {
            diagnostic::warning("`ip` expected `string`, but got `{}`",
                                arg.type.kind())
              .primary(expr)
              .emit(ctx);
            return series::null(ip_type{}, arg.length());
          },
        };
        return match(*arg.array, f);
      });
    });
  }
};

enum class check_type {
  v4,
  v6,
  multicast,
  loopback,
  private_,
  global,
  link_local
};

template <check_type CheckType>
class ip_check final : public function_plugin {
public:
  auto name() const -> std::string override {
    if constexpr (CheckType == check_type::v4) {
      return "is_v4";
    } else if constexpr (CheckType == check_type::v6) {
      return "is_v6";
    } else if constexpr (CheckType == check_type::multicast) {
      return "is_multicast";
    } else if constexpr (CheckType == check_type::loopback) {
      return "is_loopback";
    } else if constexpr (CheckType == check_type::private_) {
      return "is_private";
    } else if constexpr (CheckType == check_type::global) {
      return "is_global";
    } else if constexpr (CheckType == check_type::link_local) {
      return "is_link_local";
    }
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "ip")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr), this](evaluator eval, session ctx) -> series {
        auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
        check(b.Reserve(eval.length()));
        for (auto& arg : eval(expr)) {
          auto f = detail::overload{
            [&](const arrow::NullArray& arg) {
              check(b.AppendNulls(arg.length()));
            },
            [&](const tenzir::ip_type::array_type& arg) {
              for (const auto& value : values(tenzir::ip_type{}, arg)) {
                if (not value) {
                  check(b.AppendNull());
                  continue;
                }
                bool result = false;
                if constexpr (CheckType == check_type::v4) {
                  result = value->is_v4();
                } else if constexpr (CheckType == check_type::v6) {
                  result = value->is_v6();
                } else if constexpr (CheckType == check_type::multicast) {
                  result = value->is_multicast();
                } else if constexpr (CheckType == check_type::loopback) {
                  result = value->is_loopback();
                } else if constexpr (CheckType == check_type::private_) {
                  result = value->is_private();
                } else if constexpr (CheckType == check_type::global) {
                  result = value->is_global();
                } else if constexpr (CheckType == check_type::link_local) {
                  result = value->is_link_local();
                }
                check(b.Append(result));
              }
            },
            [&](const auto&) {
              diagnostic::warning("`{}` expected `ip`, but got `{}`", name(),
                                  arg.type.kind())
                .primary(expr)
                .emit(ctx);
              check(b.AppendNulls(arg.length()));
            },
          };
          match(*arg.array, f);
        }
        return series{bool_type{}, finish(b)};
      });
  }
};

class ip_category_plugin final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "ip_category";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("ip_category")
          .positional("x", expr, "ip")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto b = string_type::make_arrow_builder(arrow_memory_pool());
        check(b->Reserve(eval.length()));
        for (auto& arg : eval(expr)) {
          auto f = detail::overload{
            [&](const arrow::NullArray& arg) {
              for (auto i = 0; i < arg.length(); ++i) {
                check(b->AppendNull());
              }
            },
            [&](const tenzir::ip_type::array_type& arg) {
              for (const auto& value : values(tenzir::ip_type{}, arg)) {
                if (not value) {
                  check(b->AppendNull());
                  continue;
                }
                check(b->Append(to_string(value->type())));
              }
            },
            [&](const auto&) {
              diagnostic::warning("`ip_category` expected `ip`, but got `{}`",
                                  arg.type.kind())
                .primary(expr)
                .emit(ctx);
              for (auto i = 0; i < arg.length(); ++i) {
                check(b->AppendNull());
              }
            },
          };
          match(*arg.array, f);
        }
        return series{string_type{}, finish(*b)};
      });
  }
};

} // namespace

} // namespace tenzir::plugins::ip

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ip::ip)
TENZIR_REGISTER_PLUGIN(
  tenzir::plugins::ip::ip_check<tenzir::plugins::ip::check_type::v4>)
TENZIR_REGISTER_PLUGIN(
  tenzir::plugins::ip::ip_check<tenzir::plugins::ip::check_type::v6>)
TENZIR_REGISTER_PLUGIN(
  tenzir::plugins::ip::ip_check<tenzir::plugins::ip::check_type::multicast>)
TENZIR_REGISTER_PLUGIN(
  tenzir::plugins::ip::ip_check<tenzir::plugins::ip::check_type::loopback>)
TENZIR_REGISTER_PLUGIN(
  tenzir::plugins::ip::ip_check<tenzir::plugins::ip::check_type::private_>)
TENZIR_REGISTER_PLUGIN(
  tenzir::plugins::ip::ip_check<tenzir::plugins::ip::check_type::global>)
TENZIR_REGISTER_PLUGIN(
  tenzir::plugins::ip::ip_check<tenzir::plugins::ip::check_type::link_local>)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ip::ip_category_plugin)
