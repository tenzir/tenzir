//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/compute/api.h>

namespace tenzir::plugins::string {

namespace {

class starts_or_ends_with : public virtual method_plugin {
public:
  explicit starts_or_ends_with(bool starts_with) : starts_with_{starts_with} {
  }

  auto name() const -> std::string override {
    return starts_with_ ? "starts_with" : "ends_with";
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto arg_expr = ast::expression{};
    TRY(argument_parser2::method(name())
          .add(subject_expr, "<string>")
          .add(arg_expr, "<string>")
          .parse(inv, ctx));
    // TODO: This shows the need for some abstraction.
    return function_use::make([subject_expr = std::move(subject_expr),
                               arg_expr = std::move(arg_expr),
                               this](evaluator eval, session ctx) -> series {
      auto subject = eval(subject_expr);
      auto arg = eval(arg_expr);
      TENZIR_ASSERT(subject.length() == arg.length());
      auto f = detail::overload{
        [&](const arrow::StringArray& subject, const arrow::StringArray& arg) {
          auto b = arrow::BooleanBuilder{};
          check(b.Reserve(arg.length()));
          for (auto i = int64_t{0}; i < subject.length(); ++i) {
            if (subject.IsNull(i) || arg.IsNull(i)) {
              check(b.AppendNull());
              continue;
            }
            auto result = bool{};
            if (starts_with_) {
              result = subject.Value(i).starts_with(arg.Value(i));
            } else {
              result = subject.Value(i).ends_with(arg.Value(i));
            }
            check(b.Append(result));
          }
          return series{bool_type{}, finish(b)};
        },
        [&](const auto&, const auto&) {
          // TODO: Handle null array. Emit warning.
          TENZIR_UNUSED(ctx);
          return series::null(bool_type{}, subject.length());
        },
      };
      return caf::visit(f, *subject.array, *arg.array);
    });
  }

private:
  bool starts_with_;
};

class trim : public virtual method_plugin {
public:
  trim() = default;

  auto name() const -> std::string override {
    return "trim";
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto characters_expr = std::optional<ast::expression>{};
    TRY(argument_parser2::method(name())
          .add(subject_expr, "<string>")
          .add("chars", characters_expr)
          .parse(inv, ctx));
    auto options = arrow::compute::TrimOptions{" \t\n\v\f\r"};
    if (characters_expr) {
      TRY(auto characters, const_eval(*characters_expr, ctx));
      const auto* characters_str = caf::get_if<std::string>(&characters);
      if (not characters_str) {
        diagnostic::error("expected string").primary(*characters_expr).emit(ctx);
        return failure::promise();
      }
      options = arrow::compute::TrimOptions{*characters_str};
    }
    return function_use::make(
      [subject_expr = std::move(subject_expr),
       options = std::move(options)](evaluator eval, session ctx) -> series {
        auto subject = eval(subject_expr);
        auto f = detail::overload{
          [&](const arrow::StringArray& array) {
            auto trimmed_array
              = arrow::compute::CallFunction("utf8_trim", {array}, &options);
            if (not trimmed_array.ok()) {
              diagnostic::warning("{}", trimmed_array.status().ToString())
                .primary(subject_expr)
                .emit(ctx);
              return series::null(string_type{}, subject.length());
            }
            return series{string_type{},
                          trimmed_array.MoveValueUnsafe().make_array()};
          },
          [&](const auto&) {
            diagnostic::warning("`trim` expected `string`, but got `{}`",
                                subject.type.kind())
              .primary(subject_expr)
              .emit(ctx);
            return series::null(string_type{}, subject.length());
          },
        };
        return caf::visit(f, *subject.array);
      });
  }
};

} // namespace

} // namespace tenzir::plugins::string

TENZIR_REGISTER_PLUGIN(tenzir::plugins::string::starts_or_ends_with{true})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::string::starts_or_ends_with{false})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::string::trim)
