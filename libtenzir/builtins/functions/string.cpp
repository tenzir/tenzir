//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/to_string.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/array/array_binary.h>
#include <arrow/compute/api.h>
#include <arrow/util/utf8.h>
#include <re2/re2.h>

#include <iterator>
#include <string_view>

namespace tenzir::plugins::string {

namespace {

class starts_or_ends_with : public virtual function_plugin {
public:
  explicit starts_or_ends_with(bool starts_with) : starts_with_{starts_with} {
  }

  auto name() const -> std::string override {
    return starts_with_ ? "starts_with" : "ends_with";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto arg_expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", subject_expr, "string")
          .positional("prefix", arg_expr, "string")
          .parse(inv, ctx));
    // TODO: This shows the need for some abstraction.
    return function_use::make([subject_expr = std::move(subject_expr),
                               arg_expr = std::move(arg_expr),
                               this](evaluator eval, session ctx) -> series {
      TENZIR_UNUSED(ctx);
      auto b = arrow::BooleanBuilder{};
      check(b.Reserve(eval.length()));
      for (auto [subject, arg] :
           split_multi_series(eval(subject_expr), eval(arg_expr))) {
        TENZIR_ASSERT(subject.length() == arg.length());
        auto f = detail::overload{
          [&](const arrow::StringArray& subject,
              const arrow::StringArray& arg) {
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
          },
          [&](const auto&, const auto&) {
            // TODO: Handle null array. Emit warning.
            check(b.AppendNulls(arg.length()));
          },
        };
        match(std::tie(*subject.array, *arg.array), f);
      }
      return series{bool_type{}, finish(b)};
    });
  }

private:
  bool starts_with_;
};

class match_regex : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "match_regex";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto pattern = located<std::string>{};
    TRY(argument_parser2::function(name())
          .positional("input", subject_expr, "string")
          .positional("regex", pattern)
          .parse(inv, ctx));
    auto regex = std::make_unique<re2::RE2>(pattern.inner,
                                            re2::RE2::CannedOptions::Quiet);
    TENZIR_ASSERT(regex);
    if (not regex->ok()) {
      diagnostic::error("failed to parse regex: {}", regex->error())
        .primary(pattern)
        .emit(ctx);
    }
    return function_use::make(
      [this, subject_expr = std::move(subject_expr),
       regex = std::move(regex)](evaluator eval, session ctx) {
        return map_series(eval(subject_expr), [&](series subject) {
          auto f = detail::overload{
            [&](const arrow::StringArray& array) -> multi_series {
              auto b = arrow::BooleanBuilder{};
              for (auto i = int64_t{0}; i < subject.length(); ++i) {
                if (array.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                const auto v = array.Value(i);
                auto matches
                  = re2::RE2::PartialMatch({v.data(), v.size()}, *regex);
                check(b.Append(matches));
              }
              return series{bool_type{}, finish(b)};
            },
            [&](const arrow::NullArray& array) -> multi_series {
              return series::null(bool_type{}, array.length());
            },
            [&](const auto&) -> multi_series {
              diagnostic::warning("`{}` expected `string`, but got `{}`",
                                  name(), subject.type.kind())
                .primary(subject_expr)
                .emit(ctx);
              return series::null(bool_type{}, subject.length());
            },
          };
          return match(*subject.array, f);
        });
      });
  }
};

class trim : public virtual function_plugin {
public:
  explicit trim(std::string name, std::string fn_name)
    : name_{std::move(name)}, fn_name_{std::move(fn_name)} {
  }

  auto name() const -> std::string override {
    return name_;
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto characters = std::optional<std::string>{};
    TRY(argument_parser2::function(name())
          .positional("x", subject_expr, "string")
          .positional("chars", characters)
          .parse(inv, ctx));
    auto options = std::optional<arrow::compute::TrimOptions>{};
    if (characters) {
      options.emplace(std::move(*characters));
    }
    auto fn_name = options ? fn_name_ : fmt::format("{}_whitespace", fn_name_);
    return function_use::make([subject_expr = std::move(subject_expr),
                               options = std::move(options), name = name_,
                               fn_name = std::move(fn_name)](
                                evaluator eval, session ctx) -> multi_series {
      return map_series(eval(subject_expr), [&](series subject) {
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
        return match(*subject.array, f);
      });
    });
  }

private:
  std::string name_;
  std::string fn_name_;
};

class pad : public virtual function_plugin {
public:
  explicit pad(std::string name, bool pad_left)
    : name_{std::move(name)}, pad_left_{pad_left} {
  }

  auto name() const -> std::string override {
    return name_;
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto length_expr = ast::expression{};
    auto pad_char_arg = std::optional<located<std::string>>{};
    TRY(argument_parser2::function(name())
          .positional("x", subject_expr, "string")
          .positional("length", length_expr, "int")
          .positional("pad_char", pad_char_arg)
          .parse(inv, ctx));
    auto pad_char
      = pad_char_arg.value_or(located<std::string>{" ", location::unknown});
    // Validate pad character is single character
    if (pad_char_arg.has_value()) {
      int64_t pad_char_length = 0;
      auto ptr = pad_char.inner.data();
      auto end = ptr + pad_char.inner.size();
      while (ptr < end) {
        if ((*ptr & 0xC0) != 0x80) {
          pad_char_length++;
        }
        ptr++;
      }
      if (pad_char_length != 1) {
        diagnostic::error("`{}` expected single character for padding, "
                          "but got `{}` with length {}",
                          name(), pad_char.inner, pad_char_length)
          .primary(pad_char)
          .emit(ctx);
        return failure::promise();
      }
    }
    return function_use::make(
      [subject_expr = std::move(subject_expr),
       length_expr = std::move(length_expr), pad_char = std::move(pad_char),
       pad_left = pad_left_,
       name = name_](evaluator eval, session ctx) -> multi_series {
        auto b = arrow::StringBuilder{};
        for (auto [subject, length] :
             split_multi_series(eval(subject_expr), eval(length_expr))) {
          TENZIR_ASSERT(subject.length() == length.length());
          auto f = detail::overload{
            [&](const arrow::StringArray& subject_array,
                const concepts::one_of<arrow::Int64Array,
                                       arrow::UInt64Array> auto& length_array) {
              for (auto i = int64_t{0}; i < subject_array.length(); ++i) {
                if (subject_array.IsNull(i) || length_array.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                auto str = subject_array.GetView(i);
                auto target_length
                  = detail::narrow<int64_t>(length_array.Value(i));
                // For simple string length, we can use the string view's size
                // for ASCII or count UTF-8 characters manually.
                auto str_length = int64_t{0};
                auto ptr = str.data();
                auto end = ptr + str.size();
                while (ptr < end) {
                  // Skip UTF-8 continuation bytes (10xxxxxx)
                  if ((*ptr & 0xC0) != 0x80) {
                    str_length++;
                  }
                  ptr++;
                }
                if (str_length >= target_length) {
                  // String is already long enough.
                  check(b.Append(str));
                  continue;
                }
                // Calculate padding needed.
                auto padding_needed
                  = static_cast<size_t>(target_length - str_length);
                std::string result;
                result.reserve(str.size()
                               + padding_needed * pad_char.inner.size());
                if (pad_left) {
                  // Pad on the left
                  for (size_t j = 0; j < padding_needed; ++j) {
                    result += pad_char.inner;
                  }
                  result += str;
                } else {
                  // Pad on the right
                  result = str;
                  for (size_t j = 0; j < padding_needed; ++j) {
                    result += pad_char.inner;
                  }
                }
                check(b.Append(result));
              }
            },
            [&]<class T, class U>(const T&, const U&) {
              if constexpr (not detail::is_any_v<T, arrow::StringArray,
                                                 arrow::NullArray>) {
                diagnostic::warning("`{}` expected `string`, but got `{}`",
                                    name, subject.type.kind())
                  .primary(subject_expr)
                  .emit(ctx);
              }
              if constexpr (not detail::is_any_v<U, arrow::Int64Array,
                                                 arrow::UInt64Array,
                                                 arrow::NullArray>) {
                diagnostic::warning("`{}` expected `int`, but got `{}`", name,
                                    length.type.kind())
                  .primary(length_expr)
                  .emit(ctx);
              }
              check(b.AppendNulls(subject.length()));
            },
          };
          match(std::tie(*subject.array, *length.array), f);
        }

        return series{string_type{}, finish(b)};
      });
  }

private:
  std::string name_;
  bool pad_left_;
};

class nullary_method : public virtual function_plugin {
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

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto function_name() const -> std::string override {
    if (name_.ends_with("()")) {
      return name_.substr(0, name_.size() - 2);
    }
    return name_;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    // TODO: Use `result_arrow_ty` to derive type name.
    TRY(argument_parser2::function(name())
          .positional("x", subject_expr, "")
          .parse(inv, ctx));
    return function_use::make([this, subject_expr = std::move(subject_expr)](
                                evaluator eval, session ctx) {
      return map_series(eval(subject_expr), [&](series subject) {
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
        return match(*subject.array, f);
      });
    });
  }

private:
  std::string name_;
  std::string fn_name_;
  type result_ty_;
  std::shared_ptr<arrow::DataType> result_arrow_ty_;
};

class replace : public virtual function_plugin {
public:
  replace() = default;
  explicit replace(bool regex) : regex_{regex} {
  }

  auto name() const -> std::string override {
    return regex_ ? "tql2.replace_regex" : "tql2.replace_fn";
  }

  auto function_name() const -> std::string override {
    return regex_ ? "tql2.replace_regex" : "tql2.replace";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto pattern = located<std::string>{};
    auto replacement = std::string{};
    auto max_replacements = std::optional<located<int64_t>>{};
    TRY(argument_parser2::function(name())
          .positional("x", subject_expr, "string")
          .positional("pattern", pattern)
          .positional("replacement", replacement)
          .named("max", max_replacements)
          .parse(inv, ctx));
    if (max_replacements) {
      if (max_replacements->inner < 0) {
        diagnostic::error("`max` must be at least 0, but got {}",
                          max_replacements->inner)
          .primary(*max_replacements)
          .emit(ctx);
      }
    }
    return function_use::make([this, subject_expr = std::move(subject_expr),
                               pattern = std::move(pattern),
                               replacement = std::move(replacement),
                               max_replacements](evaluator eval, session ctx) {
      auto result_type = string_type{};
      auto result_arrow_type
        = std::shared_ptr<arrow::DataType>{result_type.to_arrow_type()};
      return map_series(eval(subject_expr), [&](series subject) {
        auto f = detail::overload{
          [&](const arrow::StringArray& array) {
            auto max = max_replacements ? max_replacements->inner : -1;
            auto options = arrow::compute::ReplaceSubstringOptions(
              pattern.inner, replacement, max);
            auto result = arrow::compute::CallFunction(
              regex_ ? "replace_substring_regex" : "replace_substring", {array},
              &options);
            if (not result.ok()) {
              diagnostic::warning("{}",
                                  result.status().ToStringWithoutContextLines())
                .severity(result.status().IsInvalid() ? severity::error
                                                      : severity::warning)
                .primary(pattern.source)
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
        return match(*subject.array, f);
      });
    });
  }

private:
  bool regex_ = {};
};

class slice : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.slice";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto begin = std::optional<located<int64_t>>{};
    auto end = std::optional<located<int64_t>>{};
    auto stride = std::optional<located<int64_t>>{};
    TRY(argument_parser2::function(name())
          .positional("x", subject_expr, "string")
          .named("begin", begin)
          .named("end", end)
          .named("stride", stride)
          .parse(inv, ctx));
    if (stride) {
      if (stride->inner <= 0) {
        diagnostic::error("`stride` must be greater 0, but got {}",
                          stride->inner)
          .primary(*stride)
          .emit(ctx);
      }
    }
    return function_use::make([this, subject_expr = std::move(subject_expr),
                               begin = begin, end = end,
                               stride = stride](evaluator eval, session ctx) {
      auto result_type = string_type{};
      auto result_arrow_type
        = std::shared_ptr<arrow::DataType>{result_type.to_arrow_type()};
      return map_series(eval(subject_expr), [&](series subject) {
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
        return match(*subject.array, f);
      });
    });
  }
};

template <bool Deprecated>
class string_fn : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return Deprecated ? "tql2.str" : "tql2.string";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    if constexpr (Deprecated) {
      diagnostic::warning("`str` has been renamed to `string`")
        .note("`str` alias will be removed and become a hard error in a future "
              "release")
        .primary(inv.call.get_location())
        .emit(ctx);
    }
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "any")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        return to_string(eval(expr), expr.get_location(), ctx);
      });
  }
};

class split_fn : public virtual function_plugin {
public:
  split_fn() = default;
  explicit split_fn(bool regex) : regex_{regex} {
  }

  auto name() const -> std::string override {
    return regex_ ? "tql2.split_regex" : "tql2.split";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto pattern = located<std::string>{};
    auto reverse = std::optional<location>{};
    auto max_splits = std::optional<located<int64_t>>{};
    TRY(argument_parser2::function(name())
          .positional("x", subject_expr, "string")
          .positional("pattern", pattern)
          .named("max", max_splits)
          .named("reverse", reverse)
          .parse(inv, ctx));
    if (max_splits) {
      if (max_splits->inner < 0) {
        diagnostic::error("`max` must be at least 0, but got {}",
                          max_splits->inner)
          .primary(*max_splits)
          .emit(ctx);
      }
    }
    return function_use::make([this, subject_expr = std::move(subject_expr),
                               pattern = std::move(pattern), max_splits,
                               reverse](evaluator eval, session ctx) {
      static const auto result_type = type{list_type{string_type{}}};
      static const auto result_arrow_type = result_type.to_arrow_type();
      return map_series(eval(subject_expr), [&](series subject) {
        auto f = detail::overload{
          [&](const arrow::StringArray& array) {
            auto options = arrow::compute::SplitPatternOptions();
            options.pattern = pattern.inner;
            options.max_splits = max_splits ? max_splits->inner : -1;
            options.reverse = reverse.has_value();
            auto result = arrow::compute::CallFunction(
              regex_ ? "split_pattern_regex" : "split_pattern", {array},
              &options);
            if (not result.ok()) {
              diagnostic::warning("{}",
                                  result.status().ToStringWithoutContextLines())
                .severity(result.status().IsInvalid() ? severity::error
                                                      : severity::warning)
                .primary(pattern.source)
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
        return match(*subject.array, f);
      });
    });
  }

private:
  bool regex_ = {};
};

class join : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.join";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    // TODO: Technically, this could be an expression and not just a constant
    // string.
    auto separator = std::optional<located<std::string>>{};
    TRY(argument_parser2::function(name())
          .positional("x", subject_expr, "list")
          .positional("separator", separator)
          .parse(inv, ctx));
    return function_use::make(
      [subject_expr = std::move(subject_expr),
       separator = std::move(separator)](evaluator eval, session ctx) {
        static const auto result_type = type{string_type{}};
        static const auto result_arrow_type = result_type.to_arrow_type();
        return map_series(eval(subject_expr), [&](series subject) {
          auto f = detail::overload{
            [&](const arrow::ListArray& array) {
              auto emit_null_warning = [&, warned = false]() mutable {
                if (not warned) {
                  diagnostic::warning("found `null` in list passed to `join`")
                    .primary(subject_expr)
                    .hint("consider using `.where(x => x != null)` before")
                    .emit(ctx);
                  warned = true;
                }
              };
              if (is<arrow::NullArray>(*array.values())) {
                auto b = arrow::StringBuilder{};
                check(b.Reserve(array.length()));
                for (auto i = 0; i < array.length(); ++i) {
                  if (array.IsNull(i)) {
                    check(b.AppendNull());
                    continue;
                  }
                  if (array.value_length(i) == 0) {
                    check(b.Append(""));
                  } else {
                    emit_null_warning();
                    check(b.AppendNull());
                  }
                }
                return series{result_type, finish(b)};
              }
              if (not is<arrow::StringArray>(*array.values())) {
                diagnostic::warning(
                  "`join` expected `list<string>`, but got `list<{}>`",
                  as<list_type>(subject.type).value_type().kind())
                  .primary(subject_expr)
                  .emit(ctx);
                return series::null(result_type, subject.length());
              }
              // Arrow just silently uses `null` as the result if any element of
              // the list is `null`, but we want to inform the user, hence we
              // check it ourselves here.
              for (auto i = 0; i < array.length(); ++i) {
                if (array.IsNull(i)) {
                  continue;
                }
                auto begin = array.value_offset(i);
                auto end = begin + array.value_length(i);
                for (; begin < end; ++begin) {
                  if (array.values()->IsNull(begin)) {
                    emit_null_warning();
                  }
                }
              }
              auto result = check(arrow::compute::CallFunction(
                "binary_join",
                {array, std::make_shared<arrow::StringScalar>(
                          separator ? separator->inner : "")},
                nullptr, nullptr));
              return series{result_type, result.make_array()};
            },
            [&](const arrow::NullArray& array) {
              return series::null(result_type, array.length());
            },
            [&](const auto&) {
              diagnostic::warning("`join` expected `list`, but got `{}`",
                                  subject.type.kind())
                .primary(subject_expr)
                .emit(ctx);
              return series::null(result_type, subject.length());
            },
          };
          return match(*subject.array, f);
        });
      });
  }
};

} // namespace

} // namespace tenzir::plugins::string

using namespace tenzir;
using namespace tenzir::plugins::string;

TENZIR_REGISTER_PLUGIN(starts_or_ends_with{true})
TENZIR_REGISTER_PLUGIN(starts_or_ends_with{false})

TENZIR_REGISTER_PLUGIN(match_regex)

TENZIR_REGISTER_PLUGIN(trim{"trim", "utf8_trim"})
TENZIR_REGISTER_PLUGIN(trim{"trim_start", "utf8_ltrim"})
TENZIR_REGISTER_PLUGIN(trim{"trim_end", "utf8_rtrim"})

TENZIR_REGISTER_PLUGIN(pad{"pad_start", true})
TENZIR_REGISTER_PLUGIN(pad{"pad_end", false})

TENZIR_REGISTER_PLUGIN(nullary_method{"capitalize", "utf8_capitalize",
                                      string_type{}})
TENZIR_REGISTER_PLUGIN(nullary_method{"to_lower", "utf8_lower", string_type{}})
TENZIR_REGISTER_PLUGIN(nullary_method{"reverse()", "utf8_reverse",
                                      string_type{}})
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

TENZIR_REGISTER_PLUGIN(replace{true});
TENZIR_REGISTER_PLUGIN(replace{false});
TENZIR_REGISTER_PLUGIN(slice);
TENZIR_REGISTER_PLUGIN(string_fn<false>);
TENZIR_REGISTER_PLUGIN(string_fn<true>);

TENZIR_REGISTER_PLUGIN(split_fn{true});
TENZIR_REGISTER_PLUGIN(split_fn{false});
TENZIR_REGISTER_PLUGIN(join);
