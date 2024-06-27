//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/tenzir/si.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/tql2/arrow_utils.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::ocsf {

namespace {

class category_uid final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.ocsf_category_uid";
  }

  auto make_function(invocation inv, session ctx) const
    -> std::unique_ptr<function_use> override {
    auto expr = ast::expression{};
    argument_parser2::function("ocsf_category_uid")
      .add(expr, "<string>")
      .parse(inv, ctx);
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto arg = eval(expr);
        auto f = detail::overload{
          [&](const arrow::NullArray& arg) {
            return series::null(int64_type{}, arg.length());
          },
          [](const arrow::StringArray& arg) {
            auto b = arrow::Int64Builder{};
            check(b.Reserve(arg.length()));
            for (auto i = int64_t{0}; i < arg.length(); ++i) {
              if (arg.IsNull(i)) {
                check(b.AppendNull());
                continue;
              }
              auto name = arg.GetView(i);
              auto id = std::invoke([&] {
                if (name == "System Activity") {
                  return 1;
                }
                if (name == "Findings") {
                  return 2;
                }
                if (name == "Identity & Access Management") {
                  return 3;
                }
                if (name == "Network Activity") {
                  return 4;
                }
                if (name == "Discovery") {
                  return 5;
                }
                if (name == "Application Activity") {
                  return 6;
                }
                return 0;
              });
              check(b.Append(id));
            }
            return series{int64_type{}, finish(b)};
          },
          [&](const auto&) {
            diagnostic::warning("`ocsf_category_uid` expected `string`, but "
                                "got `{}`",
                                arg.type.kind())
              .primary(expr)
              .emit(ctx);
            return series::null(int64_type{}, arg.length());
          },
        };
        return caf::visit(f, *arg.array);
      });
  }
};

class class_uid final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.ocsf_class_uid";
  }

  auto make_function(invocation inv, session ctx) const
    -> std::unique_ptr<function_use> override {
    auto expr = ast::expression{};
    argument_parser2::function("ocsf_category_uid")
      .add(expr, "<string>")
      .parse(inv, ctx);
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto arg = eval(expr);
        auto f = detail::overload{
          [&](const arrow::NullArray& arg) {
            return series::null(int64_type{}, arg.length());
          },
          [](const arrow::StringArray& arg) {
            auto b = arrow::Int64Builder{};
            check(b.Reserve(arg.length()));
            for (auto i = int64_t{0}; i < arg.length(); ++i) {
              if (arg.IsNull(i)) {
                check(b.AppendNull());
                continue;
              }
              auto name = arg.GetView(i);
              // TODO: Auto-generate a table for this!
              auto id = std::invoke([&] {
                if (name == "Process Activity") {
                  return 1007;
                }
                return 0;
              });
              check(b.Append(id));
            }
            return series{int64_type{}, finish(b)};
          },
          [&](const auto&) {
            diagnostic::warning("`ocsf_category_uid` expected `string`, but "
                                "got `{}`",
                                arg.type.kind())
              .primary(expr)
              .emit(ctx);
            return series::null(int64_type{}, arg.length());
          },
        };
        return caf::visit(f, *arg.array);
      });
  }
};

} // namespace

} // namespace tenzir::plugins::ocsf

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::category_uid)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::class_uid)
