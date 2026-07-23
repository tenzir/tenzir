//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/to_string.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/array/array_binary.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/compute/api.h>
#include <arrow/util/utf8.h>
#include <re2/re2.h>

#include <algorithm>
#include <iterator>
#include <limits>
#include <string_view>
#include <vector>

namespace tenzir::plugins::string {

namespace {

constexpr auto max_string_size
  = static_cast<size_t>(std::numeric_limits<int32_t>::max());

/// Returns a copy of `array` with each value replaced by its full Unicode case
/// folding, preserving nulls. Used for case-insensitive comparison.
auto fold_case(const arrow::StringArray& array)
  -> std::shared_ptr<arrow::StringArray> {
  auto b = arrow::StringBuilder{tenzir::arrow_memory_pool()};
  check(b.Reserve(array.length()));
  for (auto i = int64_t{0}; i < array.length(); ++i) {
    if (array.IsNull(i)) {
      check(b.AppendNull());
      continue;
    }
    check(b.Append(detail::utf8_fold_case(array.Value(i))));
  }
  return finish(b);
}

auto replace_literal_ignore_case(std::string_view input,
                                 std::string_view folded_pattern,
                                 std::string_view replacement, int64_t max)
  -> std::string {
  auto result = std::string{};
  auto pos = size_t{0};
  auto count = int64_t{0};
  for (auto [s, e] : detail::utf8_fold_case_find(input, folded_pattern)) {
    if (max >= 0 and count >= max) {
      break;
    }
    result.append(input.substr(pos, s - pos));
    result.append(replacement);
    pos = e;
    ++count;
  }
  result.append(input.substr(pos));
  return result;
}

auto replace_literal(std::string_view input, std::string_view pattern,
                     std::string_view replacement, int64_t max,
                     bool ignore_case) -> std::string {
  if (pattern.empty()) {
    return std::string{input};
  }
  if (ignore_case) {
    return replace_literal_ignore_case(input, detail::utf8_fold_case(pattern),
                                       replacement, max);
  }
  auto result = std::string{};
  auto pos = size_t{0};
  auto count = int64_t{0};
  while (max < 0 or count < max) {
    auto next = input.find(pattern, pos);
    if (next == std::string_view::npos) {
      break;
    }
    result.append(input.substr(pos, next - pos));
    result.append(replacement);
    pos = next + pattern.size();
    ++count;
  }
  result.append(input.substr(pos));
  return result;
}

auto validate_max_replacements(
  std::optional<located<int64_t>> const& max_replacements, session ctx)
  -> void {
  if (max_replacements and max_replacements->inner < 0) {
    diagnostic::error("`max` must be at least 0, but got {}",
                      max_replacements->inner)
      .primary(*max_replacements)
      .emit(ctx);
  }
}

auto replace_substring(multi_series subjects, char const* arrow_function,
                       std::string const& pattern,
                       std::string const& replacement, int64_t max,
                       location pattern_location,
                       ast::expression const& subject_expr,
                       std::string_view function_name, session ctx)
  -> multi_series {
  auto result_type = string_type{};
  return map_series(std::move(subjects), [&](series subject) {
    auto f = detail::overload{
      [&](arrow::StringArray const& array) {
        auto options
          = arrow::compute::ReplaceSubstringOptions(pattern, replacement, max);
        auto result
          = arrow::compute::CallFunction(arrow_function, {array}, &options);
        if (not result.ok()) {
          diagnostic::warning("{}",
                              result.status().ToStringWithoutContextLines())
            .severity(result.status().IsInvalid() ? severity::error
                                                  : severity::warning)
            .primary(pattern_location)
            .emit(ctx);
          return series::null(result_type, subject.length());
        }
        return series{result_type, result.MoveValueUnsafe().make_array()};
      },
      [&](arrow::NullArray const& array) {
        return series::null(result_type, array.length());
      },
      [&](auto const&) {
        diagnostic::warning("`{}` expected `string`, but got `{}`",
                            function_name, subject.type.kind())
          .primary(subject_expr)
          .emit(ctx);
        return series::null(result_type, subject.length());
      },
    };
    return match(*subject.array, f);
  });
}

auto prepare_replace_all_patterns(std::vector<std::string> patterns)
  -> std::vector<std::string> {
  std::erase_if(patterns, [](const auto& pattern) {
    return pattern.empty();
  });
  std::ranges::sort(patterns, [](const auto& lhs, const auto& rhs) {
    if (lhs.size() != rhs.size()) {
      return lhs.size() > rhs.size();
    }
    return lhs < rhs;
  });
  const auto last = std::ranges::unique(patterns);
  patterns.erase(last.begin(), last.end());
  return patterns;
}

auto replace_all_literals(std::string_view input,
                          const std::vector<std::string>& patterns,
                          std::string_view replacement) -> std::string {
  auto result = std::string{};
  auto pos = size_t{0};
  while (pos < input.size()) {
    auto matched = std::string_view{};
    for (const auto& pattern : patterns) {
      if (input.substr(pos).starts_with(pattern)) {
        matched = pattern;
        break;
      }
    }
    if (matched.empty()) {
      result.push_back(input[pos]);
      ++pos;
      continue;
    }
    result.append(replacement);
    pos += matched.size();
  }
  return result;
}

auto literal_string_arg(const ast::expression& expr) -> Option<std::string> {
  const auto* constant = try_as<ast::constant>(expr);
  if (not constant) {
    return None{};
  }
  if (const auto* result = try_as<std::string>(constant->value)) {
    return *result;
  }
  return None{};
}

auto literal_patterns_arg(const ast::expression& expr)
  -> Option<std::vector<std::string>> {
  const auto* values = try_as<ast::list>(expr);
  if (not values) {
    return None{};
  }
  auto patterns = std::vector<std::string>{};
  patterns.reserve(values->items.size());
  for (const auto& item : values->items) {
    const auto* item_expr = try_as<ast::expression>(item);
    if (not item_expr) {
      return None{};
    }
    auto pattern = literal_string_arg(*item_expr);
    if (not pattern) {
      return None{};
    }
    patterns.push_back(std::move(*pattern));
  }
  return prepare_replace_all_patterns(std::move(patterns));
}

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

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto arg_expr = ast::expression{};
    auto ignore_case = false;
    TRY(argument_parser2::function(name())
          .positional("x", subject_expr, "string")
          .positional("prefix", arg_expr, "string")
          .named_optional("ignore_case", ignore_case)
          .parse(inv, ctx));
    // TODO: This shows the need for some abstraction.
    return function_use::make([subject_expr = std::move(subject_expr),
                               arg_expr = std::move(arg_expr), ignore_case,
                               this](evaluator eval, session ctx) -> series {
      TENZIR_UNUSED(ctx);
      auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
      check(b.Reserve(eval.length()));
      for (auto [subject, arg] :
           split_multi_series(eval(subject_expr), eval(arg_expr))) {
        TENZIR_ASSERT(subject.length() == arg.length());
        auto f = detail::overload{
          [&](const arrow::StringArray& subject,
              const arrow::StringArray& arg) {
            auto subject_lc = std::shared_ptr<arrow::StringArray>{};
            auto arg_lc = std::shared_ptr<arrow::StringArray>{};
            const auto* s = &subject;
            const auto* a = &arg;
            if (ignore_case) {
              subject_lc = fold_case(subject);
              arg_lc = fold_case(arg);
              s = subject_lc.get();
              a = arg_lc.get();
            }
            for (auto i = int64_t{0}; i < s->length(); ++i) {
              if (s->IsNull(i) or a->IsNull(i)) {
                check(b.AppendNull());
                continue;
              }
              auto result = bool{};
              if (starts_with_) {
                result = s->Value(i).starts_with(a->Value(i));
              } else {
                result = s->Value(i).ends_with(a->Value(i));
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

  auto make_function(function_invocation inv, session ctx) const
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
              auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
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

  auto make_function(function_invocation inv, session ctx) const
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

  auto make_function(function_invocation inv, session ctx) const
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
                if (subject_array.IsNull(i) or length_array.IsNull(i)) {
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

class repeat : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.repeat_fn";
  }

  auto function_name() const -> std::string override {
    return "tql2.repeat";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto count_expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", subject_expr, "string")
          .positional("n", count_expr, "int")
          .parse(inv, ctx));
    return function_use::make([subject_expr = std::move(subject_expr),
                               count_expr = std::move(count_expr)](
                                evaluator eval, session ctx) -> multi_series {
      auto b = arrow::StringBuilder{tenzir::arrow_memory_pool()};
      auto total_size = size_t{0};
      for (auto [subject, count] :
           split_multi_series(eval(subject_expr), eval(count_expr))) {
        TENZIR_ASSERT(subject.length() == count.length());
        auto f = detail::overload{
          [&](arrow::StringArray const& subject_array,
              concepts::one_of<arrow::Int64Array,
                               arrow::UInt64Array> auto const& count_array) {
            for (auto i = int64_t{0}; i < subject_array.length(); ++i) {
              if (subject_array.IsNull(i) or count_array.IsNull(i)) {
                check(b.AppendNull());
                continue;
              }
              auto str = subject_array.GetView(i);
              auto n = uint64_t{};
              if constexpr (std::same_as<std::decay_t<decltype(count_array)>,
                                         arrow::Int64Array>) {
                auto value = count_array.Value(i);
                if (value < 0) {
                  diagnostic::warning("`repeat` expected non-negative count, "
                                      "but got {}",
                                      value)
                    .primary(count_expr)
                    .emit(ctx);
                  check(b.AppendNull());
                  continue;
                }
                n = static_cast<uint64_t>(value);
              } else {
                n = count_array.Value(i);
              }
              if (n == 0 or str.empty()) {
                check(b.Append(""));
                continue;
              }
              if (n > max_string_size / str.size()) {
                diagnostic::warning(
                  "`repeat` result exceeds maximum string size")
                  .primary(count_expr)
                  .emit(ctx);
                check(b.AppendNull());
                continue;
              }
              auto size = static_cast<size_t>(n) * str.size();
              if (size > max_string_size - total_size) {
                diagnostic::warning(
                  "`repeat` result exceeds maximum string array size")
                  .primary(count_expr)
                  .emit(ctx);
                check(b.AppendNull());
                continue;
              }
              auto result = std::string{};
              result.reserve(size);
              for (auto j = uint64_t{0}; j < n; ++j) {
                result += str;
              }
              check(b.Append(result));
              total_size += size;
            }
          },
          [&]<class T, class U>(T const&, U const&) {
            if constexpr (not detail::is_any_v<T, arrow::StringArray,
                                               arrow::NullArray>) {
              diagnostic::warning("`repeat` expected `string`, but got `{}`",
                                  subject.type.kind())
                .primary(subject_expr)
                .emit(ctx);
            }
            if constexpr (not detail::is_any_v<U, arrow::Int64Array,
                                               arrow::UInt64Array,
                                               arrow::NullArray>) {
              diagnostic::warning("`repeat` expected `int`, but got `{}`",
                                  count.type.kind())
                .primary(count_expr)
                .emit(ctx);
            }
            check(b.AppendNulls(subject.length()));
          },
        };
        match(std::tie(*subject.array, *count.array), f);
      }
      return series{string_type{}, finish(b)};
    });
  }
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

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    // TODO: Use `result_arrow_ty` to derive type name.
    TRY(argument_parser2::function(name())
          .positional("x", subject_expr, "")
          .parse(inv, ctx));
    return function_use::make([this, subject_expr = std::move(subject_expr)](
                                evaluator eval, session ctx) {
      auto subject = eval(subject_expr);
      TENZIR_ASSERT_EQ(subject.length(), eval.length());
      auto result = map_series(std::move(subject), [&](series subject) {
        auto f = detail::overload{
          [&](const arrow::StringArray& array) {
            TENZIR_ASSERT_EQ(subject.array->length(), array.length());
            auto result = arrow::compute::CallFunction(fn_name_, {array});
            if (not result.ok()) {
              diagnostic::warning("{}", result.status().ToString())
                .primary(subject_expr)
                .emit(ctx);
              return series::null(result_ty_, subject.length());
            }
            TENZIR_ASSERT_EQ(result->length(), array.length());
            if (not result->type()->Equals(result_arrow_ty_)) {
              result = arrow::compute::Cast(result.MoveValueUnsafe(),
                                            result_arrow_ty_);
              TENZIR_ASSERT(result.ok(), result.status().ToString());
              TENZIR_ASSERT_EQ(result->length(), array.length());
            }
            auto output = result.MoveValueUnsafe().make_array();
            TENZIR_ASSERT_EQ(output->length(), array.length());
            return series{result_ty_, std::move(output)};
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
      TENZIR_ASSERT_EQ(result.length(), eval.length());
      return result;
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

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto pattern_expr = ast::expression{};
    auto replacement_expr = ast::expression{};
    auto max_replacements = std::optional<located<int64_t>>{};
    auto ignore_case = false;
    auto parser = argument_parser2::function(name());
    parser.positional("x", subject_expr, "string");
    if (regex_) {
      auto pattern = located<std::string>{};
      auto replacement = std::string{};
      parser.positional("pattern", pattern)
        .positional("replacement", replacement)
        .named("max", max_replacements);
      TRY(parser.parse(inv, ctx));
      validate_max_replacements(max_replacements, ctx);
      return function_use::make(
        [this, subject_expr = std::move(subject_expr),
         pattern = std::move(pattern), replacement = std::move(replacement),
         max_replacements](evaluator eval, session ctx) {
          auto max = max_replacements ? max_replacements->inner : -1;
          return replace_substring(eval(subject_expr),
                                   "replace_substring_regex", pattern.inner,
                                   replacement, max, pattern.source,
                                   subject_expr, name(), ctx);
        });
    }
    parser.positional("pattern", pattern_expr, "string")
      .positional("replacement", replacement_expr, "string")
      .named("max", max_replacements)
      .named_optional("ignore_case", ignore_case);
    TRY(parser.parse(inv, ctx));
    validate_max_replacements(max_replacements, ctx);
    auto literal_pattern = literal_string_arg(pattern_expr);
    auto literal_replacement = literal_string_arg(replacement_expr);
    return function_use::make([this, subject_expr = std::move(subject_expr),
                               pattern_expr = std::move(pattern_expr),
                               replacement_expr = std::move(replacement_expr),
                               literal_pattern = std::move(literal_pattern),
                               literal_replacement
                               = std::move(literal_replacement),
                               max_replacements, ignore_case](
                                evaluator eval, session ctx) -> multi_series {
      auto result_type = string_type{};
      auto max = max_replacements ? max_replacements->inner : -1;
      if (literal_pattern and literal_replacement) {
        if (not ignore_case and not literal_pattern->empty()) {
          return replace_substring(eval(subject_expr), "replace_substring",
                                   *literal_pattern, *literal_replacement, max,
                                   pattern_expr.get_location(), subject_expr,
                                   name(), ctx);
        }
        return map_series(eval(subject_expr), [&](series subject) {
          auto f = detail::overload{
            [&](arrow::StringArray const& array) {
              auto folded_pattern
                = literal_pattern->empty()
                    ? std::string{}
                    : detail::utf8_fold_case(*literal_pattern);
              auto b = arrow::StringBuilder{tenzir::arrow_memory_pool()};
              check(b.Reserve(array.length()));
              for (auto i = int64_t{0}; i < array.length(); ++i) {
                if (array.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                if (literal_pattern->empty()) {
                  check(b.Append(array.Value(i)));
                } else {
                  check(b.Append(
                    replace_literal_ignore_case(array.Value(i), folded_pattern,
                                                *literal_replacement, max)));
                }
              }
              return series{result_type, finish(b)};
            },
            [&](arrow::NullArray const& array) {
              return series::null(result_type, array.length());
            },
            [&](auto const&) {
              diagnostic::warning("`{}` expected `string`, but got `{}`",
                                  name(), subject.type.kind())
                .primary(subject_expr)
                .emit(ctx);
              return series::null(result_type, subject.length());
            },
          };
          return match(*subject.array, f);
        });
      }
      auto warned = false;
      auto subject = eval(subject_expr);
      auto pattern = eval(pattern_expr);
      auto replacement = eval(replacement_expr);
      auto b = arrow::StringBuilder{tenzir::arrow_memory_pool()};
      check(b.Reserve(eval.length()));
      for (auto [subject, pattern, replacement] : split_multi_series(
             std::move(subject), std::move(pattern), std::move(replacement))) {
        TENZIR_ASSERT(subject.length() == pattern.length());
        TENZIR_ASSERT(subject.length() == replacement.length());
        auto f = detail::overload{
          [&](const arrow::StringArray& subject,
              const arrow::StringArray& pattern,
              const arrow::StringArray& replacement) {
            for (auto i = int64_t{0}; i < subject.length(); ++i) {
              if (subject.IsNull(i) or pattern.IsNull(i)
                  or replacement.IsNull(i)) {
                check(b.AppendNull());
                continue;
              }
              check(b.Append(replace_literal(subject.Value(i), pattern.Value(i),
                                             replacement.Value(i), max,
                                             ignore_case)));
            }
          },
          [&]<class Subject, class Pattern, class Replacement>(
            Subject const&, Pattern const&, Replacement const&) {
            if constexpr (not detail::is_any_v<Subject, arrow::StringArray,
                                               arrow::NullArray>
                          or not detail::is_any_v<Pattern, arrow::StringArray,
                                                  arrow::NullArray>
                          or not detail::is_any_v<Replacement,
                                                  arrow::StringArray,
                                                  arrow::NullArray>) {
              if (not warned) {
                warned = true;
                diagnostic::warning("`replace` expected `string`, but got "
                                    "`{}`, "
                                    "`{}`, and `{}`",
                                    subject.type.kind(), pattern.type.kind(),
                                    replacement.type.kind())
                  .primary(subject_expr)
                  .primary(pattern_expr)
                  .primary(replacement_expr)
                  .emit(ctx);
              }
            }
            check(b.AppendNulls(subject.length()));
          },
        };
        match(std::tie(*subject.array, *pattern.array, *replacement.array), f);
      }
      return series{result_type, finish(b)};
    });
  }

private:
  bool regex_ = {};
};

class replace_all : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.replace_all";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto patterns_expr = ast::expression{};
    auto replacement_expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", subject_expr, "string")
          .positional("patterns", patterns_expr, "list")
          .positional("replacement", replacement_expr, "string")
          .parse(inv, ctx));
    auto literal_patterns = literal_patterns_arg(patterns_expr);
    auto literal_replacement = literal_string_arg(replacement_expr);
    return function_use::make([subject_expr = std::move(subject_expr),
                               patterns_expr = std::move(patterns_expr),
                               replacement_expr = std::move(replacement_expr),
                               literal_patterns = std::move(literal_patterns),
                               literal_replacement
                               = std::move(literal_replacement)](evaluator eval,
                                                                 session ctx) {
      auto result_type = string_type{};
      auto subject = eval(subject_expr);
      auto replacement
        = literal_replacement
            ? multi_series{data_to_series(*literal_replacement, eval.length())}
            : eval(replacement_expr);
      auto b = arrow::StringBuilder{tenzir::arrow_memory_pool()};
      check(b.Reserve(eval.length()));
      auto warned = false;
      if (literal_patterns) {
        for (auto [subject, replacement] :
             split_multi_series(std::move(subject), std::move(replacement))) {
          TENZIR_ASSERT(subject.length() == replacement.length());
          auto f = detail::overload{
            [&](arrow::StringArray const& subject,
                arrow::StringArray const& replacement) {
              for (auto i = int64_t{0}; i < subject.length(); ++i) {
                if (subject.IsNull(i) or replacement.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                if (literal_patterns->empty()) {
                  check(b.Append(subject.Value(i)));
                  continue;
                }
                check(b.Append(replace_all_literals(
                  subject.Value(i), *literal_patterns, replacement.Value(i))));
              }
            },
            [&]<class Subject, class Replacement>(Subject const&,
                                                  Replacement const&) {
              if constexpr (not detail::is_any_v<Subject, arrow::StringArray,
                                                 arrow::NullArray>
                            or not detail::is_any_v<Replacement,
                                                    arrow::StringArray,
                                                    arrow::NullArray>) {
                if (not warned) {
                  warned = true;
                  diagnostic::warning(
                    "`replace_all` expected `string` and `string`, but "
                    "got `{}` and `{}`",
                    subject.type.kind(), replacement.type.kind())
                    .primary(subject_expr)
                    .primary(replacement_expr)
                    .emit(ctx);
                }
              }
              check(b.AppendNulls(subject.length()));
            },
          };
          match(std::tie(*subject.array, *replacement.array), f);
        }
        return series{result_type, finish(b)};
      }
      auto patterns = eval(patterns_expr);
      for (auto [subject, patterns, replacement] : split_multi_series(
             std::move(subject), std::move(patterns), std::move(replacement))) {
        TENZIR_ASSERT(subject.length() == patterns.length());
        TENZIR_ASSERT(subject.length() == replacement.length());
        auto f = detail::overload{
          [&](arrow::StringArray const& subject,
              arrow::ListArray const& pattern_lists,
              arrow::StringArray const& replacement) {
            if (not is<arrow::StringArray>(*pattern_lists.values())
                and not is<arrow::NullArray>(*pattern_lists.values())) {
              if (not warned) {
                warned = true;
                diagnostic::warning(
                  "`replace_all` expected `list<string>`, but got `list<{}>`",
                  as<list_type>(patterns.type).value_type().kind())
                  .primary(patterns_expr)
                  .emit(ctx);
              }
              check(b.AppendNulls(subject.length()));
              return;
            }
            auto* values = is<arrow::StringArray>(*pattern_lists.values())
                             ? &as<arrow::StringArray>(*pattern_lists.values())
                             : nullptr;
            for (auto i = int64_t{0}; i < subject.length(); ++i) {
              if (subject.IsNull(i) or pattern_lists.IsNull(i)
                  or replacement.IsNull(i)) {
                check(b.AppendNull());
                continue;
              }
              auto row_patterns = std::vector<std::string>{};
              auto begin = pattern_lists.value_offset(i);
              auto end = begin + pattern_lists.value_length(i);
              auto found_null = false;
              if (values) {
                row_patterns.reserve(detail::narrow<size_t>(end - begin));
                for (auto j = begin; j < end; ++j) {
                  if (values->IsNull(j)) {
                    found_null = true;
                    break;
                  }
                  row_patterns.emplace_back(values->Value(j));
                }
              } else {
                found_null = begin != end;
              }
              if (found_null) {
                check(b.AppendNull());
                continue;
              }
              row_patterns
                = prepare_replace_all_patterns(std::move(row_patterns));
              if (row_patterns.empty()) {
                check(b.Append(subject.Value(i)));
                continue;
              }
              check(b.Append(replace_all_literals(
                subject.Value(i), row_patterns, replacement.Value(i))));
            }
          },
          [&]<class Subject, class Patterns, class Replacement>(
            Subject const&, Patterns const&, Replacement const&) {
            if constexpr (not detail::is_any_v<Subject, arrow::StringArray,
                                               arrow::NullArray>
                          or not detail::is_any_v<Patterns, arrow::ListArray,
                                                  arrow::NullArray>
                          or not detail::is_any_v<Replacement,
                                                  arrow::StringArray,
                                                  arrow::NullArray>) {
              if (not warned) {
                warned = true;
                diagnostic::warning("`replace_all` expected `string`, "
                                    "`list<string>`, and "
                                    "`string`, but got `{}`, `{}`, and `{}`",
                                    subject.type.kind(), patterns.type.kind(),
                                    replacement.type.kind())
                  .primary(subject_expr)
                  .primary(patterns_expr)
                  .primary(replacement_expr)
                  .emit(ctx);
              }
            }
            check(b.AppendNulls(subject.length()));
          },
        };
        match(std::tie(*subject.array, *patterns.array, *replacement.array), f);
      }
      return series{result_type, finish(b)};
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

  auto make_function(function_invocation inv, session ctx) const
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

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto pattern = located<std::string>{};
    auto reverse = std::optional<location>{};
    auto max_splits = std::optional<located<int64_t>>{};
    auto ignore_case = false;
    auto parser = argument_parser2::function(name());
    parser.positional("x", subject_expr, "string")
      .positional("pattern", pattern)
      .named("max", max_splits)
      .named("reverse", reverse);
    if (not regex_) {
      parser.named_optional("ignore_case", ignore_case);
    }
    TRY(parser.parse(inv, ctx));
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
                               reverse,
                               ignore_case](evaluator eval, session ctx) {
      static const auto result_type = type{list_type{string_type{}}};
      static const auto result_arrow_type = result_type.to_arrow_type();
      return map_series(eval(subject_expr), [&](series subject) {
        auto f = detail::overload{
          [&](const arrow::StringArray& array) {
            // Arrow's literal `split_pattern` has no case-insensitive mode, so
            // we match the delimiter with full Unicode case folding ourselves
            // and split the original bytes.
            if (ignore_case and not regex_ and not pattern.inner.empty()) {
              auto fp = detail::utf8_fold_case(pattern.inner);
              auto max = max_splits ? max_splits->inner : -1;
              auto value_builder = std::make_shared<arrow::StringBuilder>(
                tenzir::arrow_memory_pool());
              auto b = arrow::ListBuilder{tenzir::arrow_memory_pool(),
                                          value_builder};
              for (auto i = int64_t{0}; i < array.length(); ++i) {
                if (array.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                check(b.Append());
                auto v = array.Value(i);
                auto ranges = detail::utf8_fold_case_find(v, fp);
                // `max` bounds the number of splits; `reverse` keeps the
                // rightmost ones. The output order stays left to right.
                auto first = size_t{0};
                auto last = ranges.size();
                if (max >= 0 and ranges.size() > static_cast<size_t>(max)) {
                  if (reverse.has_value()) {
                    first = ranges.size() - static_cast<size_t>(max);
                  } else {
                    last = static_cast<size_t>(max);
                  }
                }
                auto pos = size_t{0};
                for (auto k = first; k < last; ++k) {
                  auto [s, e] = ranges[k];
                  check(value_builder->Append(v.substr(pos, s - pos)));
                  pos = e;
                }
                check(value_builder->Append(v.substr(pos)));
              }
              return series{result_type, finish(b)};
            }
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

  auto make_function(function_invocation inv, session ctx) const
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

class equals : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "equals";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto left_expr = ast::expression{};
    auto right_expr = ast::expression{};
    auto ignore_case = false;
    TRY(argument_parser2::function(name())
          .positional("x", left_expr, "string")
          .positional("y", right_expr, "string")
          .named_optional("ignore_case", ignore_case)
          .parse(inv, ctx));
    return function_use::make(
      [left_expr = std::move(left_expr), right_expr = std::move(right_expr),
       ignore_case](evaluator eval, session ctx) -> series {
        auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
        check(b.Reserve(eval.length()));
        auto warned = false;
        for (auto [left, right] :
             split_multi_series(eval(left_expr), eval(right_expr))) {
          TENZIR_ASSERT(left.length() == right.length());
          // Three-valued equality consistent with the `==` operator: two nulls
          // compare equal, a null and a non-null compare unequal.
          auto append_null_aware = [&](const arrow::Array& l,
                                       const arrow::Array& r, auto&& equal_at) {
            for (auto i = int64_t{0}; i < l.length(); ++i) {
              auto ln = l.IsNull(i);
              auto rn = r.IsNull(i);
              if (ln or rn) {
                check(b.Append(ln and rn));
                continue;
              }
              check(b.Append(equal_at(i)));
            }
          };
          auto f = detail::overload{
            [&](const arrow::StringArray& l, const arrow::StringArray& r) {
              auto l_lc = std::shared_ptr<arrow::StringArray>{};
              auto r_lc = std::shared_ptr<arrow::StringArray>{};
              const auto* lp = &l;
              const auto* rp = &r;
              if (ignore_case) {
                l_lc = fold_case(l);
                r_lc = fold_case(r);
                lp = l_lc.get();
                rp = r_lc.get();
              }
              append_null_aware(l, r, [&](int64_t i) {
                return lp->Value(i) == rp->Value(i);
              });
            },
            [&](const arrow::NullArray&, const arrow::NullArray&) {
              check(b.AppendValues(left.length(), true));
            },
            [&](const arrow::StringArray& l, const arrow::NullArray&) {
              for (auto i = int64_t{0}; i < l.length(); ++i) {
                check(b.Append(l.IsNull(i)));
              }
            },
            [&](const arrow::NullArray&, const arrow::StringArray& r) {
              for (auto i = int64_t{0}; i < r.length(); ++i) {
                check(b.Append(r.IsNull(i)));
              }
            },
            [&](const auto&, const auto&) {
              if (not warned) {
                warned = true;
                diagnostic::warning("`equals` expected `string`, but got `{}` "
                                    "and `{}`",
                                    left.type.kind(), right.type.kind())
                  .primary(left_expr)
                  .primary(right_expr)
                  .emit(ctx);
              }
              check(b.AppendNulls(left.length()));
            },
          };
          match(std::tie(*left.array, *right.array), f);
        }
        return series{bool_type{}, finish(b)};
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
TENZIR_REGISTER_PLUGIN(repeat)

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
TENZIR_REGISTER_PLUGIN(replace_all);
TENZIR_REGISTER_PLUGIN(string_fn<false>);
TENZIR_REGISTER_PLUGIN(string_fn<true>);

TENZIR_REGISTER_PLUGIN(split_fn{true});
TENZIR_REGISTER_PLUGIN(split_fn{false});
TENZIR_REGISTER_PLUGIN(join);
TENZIR_REGISTER_PLUGIN(equals);
