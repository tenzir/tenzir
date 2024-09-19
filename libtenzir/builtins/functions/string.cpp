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
  explicit trim(std::string name, std::string fn_name)
    : name_{std::move(name)}, fn_name_{std::move(fn_name)} {
  }

  auto name() const -> std::string override {
    return name_;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto characters = std::optional<std::string>{};
    TRY(argument_parser2::method(name())
          .add(subject_expr, "<string>")
          .add(characters, "<characters>")
          .parse(inv, ctx));
    auto options = std::optional<arrow::compute::TrimOptions>{};
    if (characters) {
      options.emplace(std::move(*characters));
    }
    auto fn_name = options ? fn_name_ : fmt::format("{}_whitespace", fn_name_);
    return function_use::make(
      [subject_expr = std::move(subject_expr), options = std::move(options),
       name = name_,
       fn_name = std::move(fn_name)](evaluator eval, session ctx) -> series {
        auto subject = eval(subject_expr);
        auto f = detail::overload{
          [&](const arrow::StringArray& array) {
            auto trimmed_array = arrow::compute::CallFunction(
              fn_name, {array}, options ? &*options : nullptr);
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
            diagnostic::warning("`{}` expected `string`, but got `{}`", name,
                                subject.type.kind())
              .primary(subject_expr)
              .emit(ctx);
            return series::null(string_type{}, subject.length());
          },
        };
        return caf::visit(f, *subject.array);
      });
  }

private:
  std::string name_;
  std::string fn_name_;
};

class nullary_method : public virtual method_plugin {
public:
  nullary_method(std::string name, std::string fn_name, type result_ty)
    : name_{std::move(name)},
      fn_name_{std::move(fn_name)},
      result_ty_{std::move(result_ty)},
      result_arrow_ty_{result_ty_.to_arrow_type()} {
  }

  template <concrete_type Ty>
  nullary_method(std::string name, std::string fn_name, Ty result_ty)
    : nullary_method(std::move(name), std::move(fn_name),
                     type{std::move(result_ty)}) {
  }

  auto name() const -> std::string override {
    return name_;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    TRY(argument_parser2::method(name())
          .add(subject_expr, "<string>")
          .parse(inv, ctx));
    return function_use::make([this, subject_expr = std::move(subject_expr)](
                                evaluator eval, session ctx) -> series {
      auto subject = eval(subject_expr);
      auto f = detail::overload{
        [&](const arrow::StringArray& array) {
          auto result = arrow::compute::CallFunction(fn_name_, {array});
          if (not result.ok()) {
            diagnostic::warning("{}", result.status().ToString())
              .primary(subject_expr)
              .emit(ctx);
            return series::null(result_ty_, subject.length());
          }
          if (not result->type()->Equals(result_arrow_ty_)) {
            result = arrow::compute::Cast(result.MoveValueUnsafe(),
                                          result_arrow_ty_);
            TENZIR_ASSERT(result.ok(), result.status().ToString());
          }
          return series{result_ty_, result.MoveValueUnsafe().make_array()};
        },
        [&](const arrow::NullArray& array) {
          return series::null(result_ty_, array.length());
        },
        [&](const auto&) {
          diagnostic::warning("`{}` expected `string`, but got `{}`", name_,
                              subject.type.kind())
            .primary(subject_expr)
            .emit(ctx);
          return series::null(result_ty_, subject.length());
        },
      };
      return caf::visit(f, *subject.array);
    });
  }

private:
  std::string name_;
  std::string fn_name_;
  type result_ty_;
  std::shared_ptr<arrow::DataType> result_arrow_ty_;
};

class replace : public virtual method_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.replace";
  }

  auto make_function(invocation inv,
                     session ctx) const -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto pattern = std::string{};
    auto replacement = std::string{};
    auto max_replacements = std::optional<located<int64_t>>{};
    TRY(argument_parser2::method(name())
          .add(subject_expr, "<string>")
          .add(pattern, "<pattern>")
          .add(replacement, "<replacement>")
          .add("max", max_replacements)
          .parse(inv, ctx));
    if (max_replacements) {
      if (max_replacements->inner < 0) {
        diagnostic::error("`max` must be at least 0, but got {}",
                          max_replacements->inner)
          .primary(*max_replacements)
          .emit(ctx);
      }
    }
    return function_use::make(
      [this, subject_expr = std::move(subject_expr),
       pattern = std::move(pattern), replacement = std::move(replacement),
       max_replacements](evaluator eval, session ctx) -> series {
        auto result_type = string_type{};
        auto result_arrow_type
          = std::shared_ptr<arrow::DataType>{result_type.to_arrow_type()};
        auto subject = eval(subject_expr);
        auto f = detail::overload{
          [&](const arrow::StringArray& array) {
            auto max = max_replacements ? max_replacements->inner : -1;
            auto options = arrow::compute::ReplaceSubstringOptions(
              pattern, replacement, max);
            auto result = arrow::compute::CallFunction("replace_substring",
                                                       {array}, &options);
            if (not result.ok()) {
              diagnostic::warning("{}", result.status().ToString())
                .primary(subject_expr)
                .emit(ctx);
              return series::null(result_type, subject.length());
            }
            return series{result_type, result.MoveValueUnsafe().make_array()};
          },
          [&](const arrow::NullArray& array) {
            return series::null(result_type, array.length());
          },
          [&](const auto&) {
            diagnostic::warning("`{}` expected `string`, but got `{}`", name(),
                                subject.type.kind())
              .primary(subject_expr)
              .emit(ctx);
            return series::null(result_type, subject.length());
          },
        };
        return caf::visit(f, *subject.array);
      });
  }
};

class slice : public virtual method_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.slice";
  }

  auto make_function(invocation inv,
                     session ctx) const -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto begin = std::optional<located<int64_t>>{};
    auto end = std::optional<located<int64_t>>{};
    auto stride = std::optional<located<int64_t>>{};
    TRY(argument_parser2::method(name())
          .add(subject_expr, "<string>")
          .add("begin", begin)
          .add("end", end)
          .add("stride", stride)
          .parse(inv, ctx));
    if (stride) {
      if (stride->inner <= 0) {
        diagnostic::error("`stride` must be greater 0, but got {}",
                          stride->inner)
          .primary(*stride)
          .emit(ctx);
      }
    }
    return function_use::make(
      [this, subject_expr = std::move(subject_expr), begin = begin, end = end,
       stride = stride](evaluator eval, session ctx) -> series {
        auto result_type = string_type{};
        auto result_arrow_type
          = std::shared_ptr<arrow::DataType>{result_type.to_arrow_type()};
        auto subject = eval(subject_expr);
        auto f = detail::overload{
          [&](const arrow::StringArray& array) {
            auto options = arrow::compute::SliceOptions(
              begin ? begin->inner : 0,
              end ? end->inner : std::numeric_limits<int64_t>::max(),
              stride ? stride->inner : 1);
            auto result = arrow::compute::CallFunction("utf8_slice_codeunits",
                                                       {array}, &options);
            if (not result.ok()) {
              diagnostic::warning("{}", result.status().ToString())
                .primary(subject_expr)
                .emit(ctx);
              return series::null(result_type, subject.length());
            }
            return series{result_type, result.MoveValueUnsafe().make_array()};
          },
          [&](const arrow::NullArray& array) {
            return series::null(result_type, array.length());
          },
          [&](const auto&) {
            diagnostic::warning("`{}` expected `string`, but got `{}`", name(),
                                subject.type.kind())
              .primary(subject_expr)
              .emit(ctx);
            return series::null(result_type, subject.length());
          },
        };
        return caf::visit(f, *subject.array);
      });
  }
};

} // namespace

} // namespace tenzir::plugins::string

using namespace tenzir;
using namespace tenzir::plugins::string;

TENZIR_REGISTER_PLUGIN(starts_or_ends_with{true})
TENZIR_REGISTER_PLUGIN(starts_or_ends_with{false})

TENZIR_REGISTER_PLUGIN(trim{"trim", "utf8_trim"})
TENZIR_REGISTER_PLUGIN(trim{"trim_start", "utf8_ltrim"})
TENZIR_REGISTER_PLUGIN(trim{"trim_end", "utf8_rtrim"})

TENZIR_REGISTER_PLUGIN(nullary_method{"capitalize", "utf8_capitalize",
                                      string_type{}})
TENZIR_REGISTER_PLUGIN(nullary_method{"to_lower", "utf8_lower", string_type{}})
TENZIR_REGISTER_PLUGIN(nullary_method{"reverse", "utf8_reverse", string_type{}})
TENZIR_REGISTER_PLUGIN(nullary_method{"to_title", "utf8_title", string_type{}})
TENZIR_REGISTER_PLUGIN(nullary_method{"to_upper", "utf8_upper", string_type{}})

TENZIR_REGISTER_PLUGIN(nullary_method{"is_alnum", "utf8_is_alnum", bool_type{}})
TENZIR_REGISTER_PLUGIN(nullary_method{"is_alpha", "utf8_is_alpha", bool_type{}})
TENZIR_REGISTER_PLUGIN(nullary_method{"is_lower", "utf8_is_lower", bool_type{}})
TENZIR_REGISTER_PLUGIN(nullary_method{"is_numeric", "utf8_is_numeric",
                                      bool_type{}})
TENZIR_REGISTER_PLUGIN(nullary_method{"is_printable", "utf8_is_printable",
                                      bool_type{}})
TENZIR_REGISTER_PLUGIN(nullary_method{"is_title", "utf8_is_title", bool_type{}})
TENZIR_REGISTER_PLUGIN(nullary_method{"is_upper", "utf8_is_upper", bool_type{}})

TENZIR_REGISTER_PLUGIN(nullary_method{"length_bytes", "binary_length",
                                      int64_type{}});
TENZIR_REGISTER_PLUGIN(nullary_method{"length_chars", "utf8_length",
                                      int64_type{}});

TENZIR_REGISTER_PLUGIN(replace);
TENZIR_REGISTER_PLUGIN(slice);
