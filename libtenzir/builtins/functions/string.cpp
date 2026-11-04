//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/array.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/eval_kernel.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/function_plugin.hpp"
#include "tenzir/nova/stringify.hpp"
#include "tenzir/nova/type_system.hpp"
#include "tenzir/unicode.hpp"

#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/printable/to_string.hpp>
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
#include <fmt/format.h>
#include <re2/re2.h>

#include <algorithm>
#include <array>
#include <bitset>
#include <iterator>
#include <limits>
#include <span>
#include <string_view>
#include <vector>

namespace tenzir::plugins::string {

namespace {

/// The strings of an array share one buffer with 32-bit offsets. Check both
/// individual results and the accumulated bytes before appending.
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
    check(b.Append(unicode::utf8_fold_case(array.Value(i))));
  }
  return finish(b);
}

/// Appends `text` to `result` unless that would grow `result` beyond
/// `size_limit` bytes. Returns whether the append happened.
auto append_within_limit(std::string& result, std::string_view text,
                         size_t size_limit) -> bool {
  if (text.size() > size_limit - result.size()) {
    return false;
  }
  result.append(text);
  return true;
}

auto replace_literal_ignore_case(std::string_view input,
                                 std::string_view folded_pattern,
                                 std::string_view replacement, int64_t max,
                                 size_t size_limit) -> Option<std::string> {
  auto result = std::string{};
  auto pos = size_t{0};
  auto count = int64_t{0};
  for (auto [s, e] : unicode::utf8_fold_case_find(input, folded_pattern)) {
    if (max >= 0 and count >= max) {
      break;
    }
    if (not append_within_limit(result, input.substr(pos, s - pos), size_limit)
        or not append_within_limit(result, replacement, size_limit)) {
      return None{};
    }
    pos = e;
    ++count;
  }
  if (not append_within_limit(result, input.substr(pos), size_limit)) {
    return None{};
  }
  return result;
}

auto replace_literal(std::string_view input, std::string_view pattern,
                     std::string_view replacement, int64_t max,
                     bool ignore_case, size_t size_limit)
  -> Option<std::string> {
  if (pattern.empty()) {
    if (input.size() > size_limit) {
      return None{};
    }
    return std::string{input};
  }
  if (ignore_case) {
    return replace_literal_ignore_case(input, unicode::utf8_fold_case(pattern),
                                       replacement, max, size_limit);
  }
  auto result = std::string{};
  auto pos = size_t{0};
  auto count = int64_t{0};
  while (max < 0 or count < max) {
    auto next = input.find(pattern, pos);
    if (next == std::string_view::npos) {
      break;
    }
    if (not append_within_limit(result, input.substr(pos, next - pos),
                                size_limit)
        or not append_within_limit(result, replacement, size_limit)) {
      return None{};
    }
    pos = next + pattern.size();
    ++count;
  }
  if (not append_within_limit(result, input.substr(pos), size_limit)) {
    return None{};
  }
  return result;
}

auto validate_max_replacements(Option<located<int64_t>> const& max_replacements,
                               diagnostic_handler& dh) -> failure_or<void> {
  if (max_replacements and max_replacements->inner < 0) {
    diagnostic::error("`max` must be at least 0, but got {}",
                      max_replacements->inner)
      .primary(*max_replacements)
      .emit(dh);
    return failure::promise();
  }
  return {};
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

auto literal_string_arg(ast::expression const& expr) -> Option<std::string> {
  auto const* constant = try_as<ast::constant>(expr);
  if (not constant) {
    return None{};
  }
  if (auto const* result = try_as<std::string>(constant->value)) {
    return *result;
  }
  return None{};
}

/// The kernel view types of string arguments that may also be null.
template <class T>
concept StringOrNull
  = std::same_as<T, std::string_view> or std::same_as<T, nova::Null>;

/// Whether `input` is valid UTF-8. ASCII input skips the full validation.
auto valid_utf8(std::string_view input) -> bool {
  return unicode::is_ascii(input) or unicode::is_valid_utf8(input);
}

/// Evaluates a scalar-valued `f` for the string rows of `args.x`. Null rows
/// stay null silently, and rows of other types get the kernel's type warning.
template <class Args, class F>
auto eval_strings(Args const& args, nova::EvalFrame frame, F f)
  -> nova::Array<nova::Data> {
  using Result = std::invoke_result_t<F&, std::string_view>;
  static_assert(
    not std::same_as<typename Result::value_type, std::string>
    and not std::same_as<typename Result::value_type, std::string_view>);
  return nova::apply_kernel<1>(
    frame, args.name, {args.x}, args.call,
    detail::overload{
      [](diagnostic_handler&, nova::Null) -> Result {
        return None{};
      },
      [&](diagnostic_handler&, std::string_view input) -> Result {
        return f(input);
      },
    });
}

/// Like `eval_strings` for an `f` that returns `None` exactly for input that
/// is not valid UTF-8, which becomes null with a warning.
template <class Args, class F>
auto eval_utf8(Args const& args, nova::EvalFrame frame, F f)
  -> nova::Array<nova::Data> {
  auto warn_invalid = nova::WarnOnce{};
  return eval_strings(args, frame, [&](std::string_view input) {
    auto result = f(input);
    if (not result) {
      warn_invalid(frame,
                   diagnostic::warning("`{}` expected valid UTF-8", args.name)
                     .primary(args.x.source));
    }
    return result;
  });
}

using CharacterBuilder = nova::storage::DataOwner<char[]>::Builder;

/// Appends directly to the output column, bounded by its 32-bit offsets.
auto append_characters(CharacterBuilder& output, std::string_view text)
  -> bool {
  if (text.size() > max_string_size - static_cast<size_t>(output.size())) {
    return false;
  }
  if (not text.empty()) {
    output.move_append(text.begin(), text.end());
  }
  return true;
}

/// Appends literal replacements directly to the output column.
auto append_replacements(std::string_view input, std::string_view pattern,
                         std::string_view replacement, int64_t max,
                         bool ignore_case, CharacterBuilder& output) -> bool {
  if (pattern.empty()) {
    return append_characters(output, input);
  }
  auto pos = size_t{0};
  auto count = int64_t{0};
  auto fits = true;
  auto replace = [&](size_t begin, size_t end) {
    if (max >= 0 and count >= max) {
      return false;
    }
    fits = append_characters(output, input.substr(pos, begin - pos))
           and append_characters(output, replacement);
    if (not fits) {
      return false;
    }
    pos = end;
    ++count;
    return true;
  };
  if (ignore_case) {
    auto const folded = unicode::utf8_fold_case(pattern);
    for (auto [begin, end] : unicode::utf8_fold_case_find(input, folded)) {
      if (not replace(begin, end)) {
        break;
      }
    }
  } else {
    while (max < 0 or count < max) {
      auto const next = input.find(pattern, pos);
      if (next == std::string_view::npos
          or not replace(next, next + pattern.size())) {
        break;
      }
    }
  }
  return fits and append_characters(output, input.substr(pos));
}

/// Resolves the string alternative without reading rows outside the frame.
/// Preserve the kernel diagnostics for each rejected, non-null type.
template <class Args>
auto string_rows(Args const& args, nova::EvalFrame frame)
  -> Option<nova::MaskedArray<nova::Array<nova::String>>> {
  using namespace nova;
  auto warn
    = [&]<data_type Tag>(Array<Tag> const&, storage::BitMap const& mask) {
        if constexpr (not std::same_as<Tag, String>
                      and not std::same_as<Tag, Null>) {
          if ((frame.mask() & mask).any()) {
            warn_rejected_kernel_types(
              frame, args.name, args.call,
              std::array{data_type_list::unique_index_of<Tag>});
          }
        }
      };
  match(
    args.x.data,
    [&]<data_type Tag>(Array<Tag> const& array) {
      warn(array, frame.mask());
    },
    [&](UnionArray const& array) {
      for (auto const& field : array.fields()) {
        match(field.data, [&](auto const& values) {
          warn(values, field.present);
        });
      }
    });
  auto result = args.x.data.template get_alternative<String>();
  if (result) {
    result->present = result->present & frame.mask();
  }
  return result;
}

/// Builds a string column without intermediate row strings. Failed rows
/// roll back their bytes; the operation owns its diagnostics.
template <nova::data_type Tag, class Append>
auto build_strings(Option<nova::MaskedArray<nova::Array<Tag>>> subject,
                   nova::EvalFrame frame, Append append)
  -> nova::Array<nova::Data> {
  using namespace nova;
  if (subject) {
    subject->present = subject->present & frame.mask();
  }
  if (not subject or not subject->present.any()) {
    return frame.null();
  }
  auto data = CharacterBuilder{};
  auto ranges = storage::DataOwner<storage::Span[]>::make_uninitialized(
    subject->data.length());
  auto failed = storage::BitMap::Mutable{subject->data.length()};
  for (auto row : storage::bitmap_iteration(subject->present)) {
    auto const begin = data.size();
    if (row and not append(*row, kernel_arg(subject->data, *row), data)) {
      if (data.size() != begin) {
        data.truncate(begin);
      }
      failed.set(*row, true);
    }
    ranges.emplace_back(begin, data.size());
  }
  auto result = Array<String>{
    Type<String>::PrimaryPhysicalStorage{data.finish(), ranges.finish()}};
  return Array<Data>{std::move(result)}.null_where(
    frame.mask().and_not(subject->present) | std::move(failed).finish());
}

/// `append` returns true on success, false for a full column, and None for
/// invalid UTF-8. Scalar-valued functions continue to use `eval_utf8`.
template <class Args, class Append>
auto transform_strings(Args const& args, nova::EvalFrame frame, Append append)
  -> nova::Array<nova::Data> {
  auto warn_invalid = nova::WarnOnce{};
  auto warn_too_large = nova::WarnOnce{};
  return build_strings(
    string_rows(args, frame), frame,
    [&](nova::storage::Index, std::string_view input,
        CharacterBuilder& output) {
      auto const result = append(input, output);
      if (not result) {
        warn_invalid(frame,
                     diagnostic::warning("`{}` expected valid UTF-8", args.name)
                       .primary(args.x.source));
      } else if (not *result) {
        warn_too_large(frame, diagnostic::warning("{} result exceeds maximum "
                                                  "string array size",
                                                  args.name)
                                .primary(args.call));
      }
      return result and *result;
    });
}

/// Resolves type combinations once per column, without dispatching each row
/// through a scalar kernel or materializing intermediate string results.
template <size_t K, class Accept>
auto accepted_rows(nova::EvalFrame frame, std::string_view name, location call,
                   std::array<nova::ValueArgument, K> const& args,
                   Accept accept) -> nova::storage::BitMap {
  using namespace nova;
  auto arrays = [&]<size_t... Is>(std::index_sequence<Is...>) {
    return std::array{args[Is].data...};
  }(std::make_index_sequence<K>{});
  auto result = storage::BitMap{frame.mask().length(), false};
  resolve_rejected_tags(arrays, 0, std::array<size_t, K>{}, frame.mask(),
                        [&](auto const& tags, storage::BitMap const& mask) {
                          if (not mask.any()) {
                            return;
                          }
                          if (accept(tags)) {
                            result = std::move(result) | mask;
                          } else {
                            warn_rejected_kernel_types(frame, name, call, tags);
                          }
                        });
  return result;
}

auto string_or_null(size_t tag) -> bool {
  return tag == nova::data_type_list::unique_index_of<nova::String>
         or tag == nova::data_type_list::unique_index_of<nova::Null>;
}

auto string_and_count(std::array<size_t, 2> const& tags) -> bool {
  return string_or_null(tags[0])
         and (tags[1] == nova::data_type_list::unique_index_of<nova::Int>
              or tags[1] == nova::data_type_list::unique_index_of<nova::UInt>
              or tags[1] == nova::data_type_list::unique_index_of<nova::Null>);
}

/// Calls `f` with every code point of `input` and its properties until it
/// returns false. Returns whether it never did, or `None` if `input` is not
/// valid UTF-8, including after the code point that stopped the iteration.
template <class F>
auto for_each_code_point(std::string_view input, unicode::Table const& table,
                         F f) -> Option<bool> {
  if (unicode::is_ascii(input)) {
    for (auto c : input) {
      auto const code_point = static_cast<char32_t>(static_cast<uint8_t>(c));
      if (not f(code_point, unicode::ascii_properties(code_point))) {
        return false;
      }
    }
    return true;
  }
  auto const* pos = input.data();
  auto const* const end = pos + input.size();
  while (pos < end) {
    auto const code_point = unicode::decode(pos, end);
    if (not code_point) {
      return None{};
    }
    if (not f(*code_point, table.properties(*code_point))) {
      // The result is decided, but the rest must be valid as well.
      if (not valid_utf8({pos, static_cast<size_t>(end - pos)})) {
        return None{};
      }
      return false;
    }
  }
  return true;
}

/// Whether `all` holds for every code point of `input` and `any` for at least
/// one, or `None` if `input` is not valid UTF-8. An empty input satisfies no
/// `any`.
template <class All, class Any>
auto all_and_any(std::string_view input, unicode::Table const& table, All all,
                 Any any) -> Option<bool> {
  auto result_any = false;
  auto const result_all = for_each_code_point(
    input, table, [&](char32_t code_point, unicode::Properties properties) {
      result_any |= any(code_point, properties);
      return all(code_point, properties);
    });
  if (not result_all) {
    return None{};
  }
  return *result_all and result_any;
}

/// Whether `input` is titlecased: it has a cased code point, every cased code
/// point that is not lowercase follows an uncased one, and every lowercase
/// code point follows a cased one. Returns `None` if `input` is not valid
/// UTF-8.
auto is_titlecased(std::string_view input, unicode::Table const& table)
  -> Option<bool> {
  auto previous_cased = false;
  auto any_title = false;
  auto const ordered = for_each_code_point(
    input, table, [&](char32_t, unicode::Properties properties) {
      auto const in_order = properties.lower   ? previous_cased
                            : properties.cased ? not previous_cased
                                               : true;
      any_title |= properties.cased and not properties.lower;
      previous_cased = properties.cased;
      return in_order;
    });
  if (not ordered) {
    return None{};
  }
  return *ordered and any_title;
}

enum class CaseMapping { upper, lower, capitalize, title };

/// Maps the case of `input` into `output` with the simple case mappings of
/// each code point. `capitalize` uppercases the first code point and
/// lowercases the others. `title` uppercases every cased code point after an
/// uncased one and lowercases every cased code point after a cased one.
/// Returns None for invalid UTF-8, or false if the output column is full.
template <CaseMapping Mapping>
auto map_case(std::string_view input, unicode::Table const& table,
              CharacterBuilder& output) -> Option<bool> {
  // Whether to uppercase a code point, given whether it is the first one and
  // whether its predecessor is cased.
  auto const uppercase = [](bool first, bool previous_cased) {
    switch (Mapping) {
      case CaseMapping::upper:
        return true;
      case CaseMapping::lower:
        return false;
      case CaseMapping::capitalize:
        return first;
      case CaseMapping::title:
        return not previous_cased;
    }
    TENZIR_UNREACHABLE();
  };
  if (unicode::is_ascii(input)) {
    if (input.size() > max_string_size - static_cast<size_t>(output.size())) {
      return false;
    }
    if (input.empty()) {
      return true;
    }
    auto const begin = output.size();
    output.append_n(static_cast<nova::storage::Index>(input.size()), '\0');
    auto* buffer = &output[begin];
    for (auto i = size_t{0}; i < input.size(); ++i) {
      auto const c = static_cast<char32_t>(static_cast<uint8_t>(input[i]));
      auto const previous_cased
        = i > 0
          and unicode::ascii_properties(static_cast<uint8_t>(input[i - 1]))
                .cased;
      buffer[i] = static_cast<char>(uppercase(i == 0, previous_cased)
                                      ? unicode::ascii_upper(c)
                                      : unicode::ascii_lower(c));
    }
    return true;
  }
  auto const* pos = input.data();
  auto const* const end = pos + input.size();
  auto previous_cased = false;
  while (pos < end) {
    auto const first = pos == input.data();
    auto const code_point = unicode::decode(pos, end);
    if (not code_point) {
      return None{};
    }
    auto encoded = std::array<char, 4>{};
    auto const* last = unicode::encode(uppercase(first, previous_cased)
                                         ? table.upper(*code_point)
                                         : table.lower(*code_point),
                                       encoded.data());
    if (not append_characters(output, {encoded.data(), last})) {
      return false;
    }
    if constexpr (Mapping == CaseMapping::title) {
      previous_cased = table.properties(*code_point).cased;
    }
  }
  return true;
}

/// Reverses code points directly into the output column.
auto reverse_code_points(std::string_view input, CharacterBuilder& output)
  -> Option<bool> {
  if (not valid_utf8(input)) {
    return None{};
  }
  if (input.size() > max_string_size - static_cast<size_t>(output.size())) {
    return false;
  }
  if (input.empty()) {
    return true;
  }
  auto const begin = output.size();
  output.append_n(static_cast<nova::storage::Index>(input.size()), '\0');
  auto* buffer = &output[begin];
  if (unicode::is_ascii(input)) {
    std::ranges::reverse_copy(input, buffer);
    return true;
  }
  constexpr auto lead_byte_sizes
    = std::array<uint8_t, 16>{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 3, 4};
  for (auto i = size_t{0}; i < input.size();) {
    auto const end = i + lead_byte_sizes[static_cast<uint8_t>(input[i]) >> 4];
    std::copy(input.data() + i, input.data() + end,
              buffer + input.size() - end);
    i = end;
  }
  return true;
}

/// Removes the code points for which `trimmed` holds from the start and the
/// end of `input`. Returns `None` if `input` is not valid UTF-8.
template <class Trimmed>
auto trim_code_points(std::string_view input, bool start, bool end,
                      Trimmed trimmed) -> Option<std::string_view> {
  auto const* first = input.data();
  auto const* last = first + input.size();
  if (start) {
    while (first < last) {
      auto const* next = first;
      auto const code_point = unicode::decode(next, last);
      if (not code_point) {
        return None{};
      }
      if (not trimmed(*code_point)) {
        break;
      }
      first = next;
    }
  }
  if (end) {
    while (last > first) {
      auto const* previous = last;
      auto const code_point = unicode::decode_backward(first, previous);
      if (not code_point) {
        return None{};
      }
      if (not trimmed(*code_point)) {
        break;
      }
      last = previous;
    }
  }
  // Trimming decoded only the code points up to the kept ones, so the kept
  // range must be valid as well.
  auto const kept = std::string_view{first, last};
  if (not valid_utf8(kept)) {
    return None{};
  }
  return kept;
}

/// Compiles a constant regular expression argument.
auto compile_regex(located<std::string> const& pattern, diagnostic_handler& dh)
  -> failure_or<std::shared_ptr<re2::RE2 const>> {
  auto regex = std::make_shared<re2::RE2 const>(pattern.inner,
                                                re2::RE2::CannedOptions::Quiet);
  if (not regex->ok()) {
    diagnostic::error("failed to parse regex: {}", regex->error())
      .primary(pattern)
      .emit(dh);
    return failure::promise();
  }
  return regex;
}

/// Returns the byte size of the code point at `pos`, which must be before
/// `end`, or 1 if it is not valid UTF-8.
auto code_point_size(char const* pos, char const* end) -> size_t {
  auto const lead = static_cast<uint8_t>(*pos);
  auto const size = lead < 0xc0   ? size_t{1}
                    : lead < 0xe0 ? size_t{2}
                    : lead < 0xf0 ? size_t{3}
                    : lead < 0xf8 ? size_t{4}
                                  : size_t{1};
  if (size > static_cast<size_t>(end - pos)
      or not unicode::is_valid_utf8({pos, size})) {
    return 1;
  }
  return size;
}

/// Calls `f` with the submatches of every match of `regex` in `input`, from
/// left to right and with the full match first, until `f` returns false.
/// Matches see the whole input as context, never overlap, and skip an empty
/// match directly at the end of the previous match, like
/// `RE2::GlobalReplace`.
template <class F>
auto for_each_match(std::string_view input, re2::RE2 const& regex,
                    std::span<absl::string_view> submatches, F f) -> void {
  auto const* pos = input.data();
  auto const* const end = pos + input.size();
  auto const* previous_end = static_cast<char const*>(nullptr);
  while (pos <= end) {
    if (not regex.Match(input, static_cast<size_t>(pos - input.data()),
                        input.size(), re2::RE2::UNANCHORED, submatches.data(),
                        static_cast<int>(submatches.size()))) {
      return;
    }
    auto const match = submatches[0];
    if (match.empty() and match.data() == previous_end) {
      if (pos == end) {
        return;
      }
      pos += code_point_size(pos, end);
      continue;
    }
    if (not f(std::span<absl::string_view const>{submatches})) {
      return;
    }
    pos = match.data() + match.size();
    previous_end = pos;
  }
}

/// Returns the size of `rewrite` with its substitutions from `matches`, as
/// `RE2::Rewrite` appends it. The rewrite must be valid for the matches.
auto rewrite_size(std::string_view rewrite,
                  std::span<absl::string_view const> matches) -> size_t {
  auto size = size_t{0};
  for (auto i = size_t{0}; i < rewrite.size(); ++i) {
    if (rewrite[i] != '\\') {
      ++size;
      continue;
    }
    // A valid rewrite escapes only a backslash or a group number.
    ++i;
    TENZIR_ASSERT_LT(i, rewrite.size());
    size += rewrite[i] == '\\'
              ? 1
              : matches[static_cast<size_t>(rewrite[i] - '0')].size();
  }
  return size;
}

/// Replaces the first `max` matches of `regex` in `input` with `rewrite`, or
/// all matches if `max` is `None`, writing the result into `output`. Returns
/// false, without building all of it, if the output column is full.
/// The rewrite must be valid for `regex`.
auto replace_matches(std::string_view input, re2::RE2 const& regex,
                     std::string_view rewrite, Option<int64_t> max,
                     CharacterBuilder& output) -> bool {
  auto submatches = std::array<absl::string_view, 10>{};
  auto const count = 1 + re2::RE2::MaxSubmatch(rewrite);
  TENZIR_ASSERT(count <= static_cast<int>(submatches.size()));
  auto const* copied = input.data();
  auto replaced = int64_t{0};
  auto fits = true;
  if (not max or *max > 0) {
    for_each_match(input, regex,
                   std::span{submatches}.first(static_cast<size_t>(count)),
                   [&](std::span<absl::string_view const> matches) {
                     auto const match = matches[0];
                     auto const prefix = std::string_view{copied, match.data()};
                     fits = prefix.size() + rewrite_size(rewrite, matches)
                            <= max_string_size - output.size();
                     if (not fits) {
                       return false;
                     }
                     output.move_append(prefix.begin(), prefix.end());
                     for (auto i = size_t{0}; i < rewrite.size(); ++i) {
                       if (rewrite[i] != '\\') {
                         output.emplace_back(rewrite[i]);
                         continue;
                       }
                       ++i;
                       if (rewrite[i] == '\\') {
                         output.emplace_back('\\');
                       } else {
                         auto const group
                           = matches[static_cast<size_t>(rewrite[i] - '0')];
                         if (not group.empty()) {
                           output.move_append(group.begin(), group.end());
                         }
                       }
                     }
                     copied = match.data() + match.size();
                     ++replaced;
                     return not max or replaced < *max;
                   });
  }
  return fits
         and append_characters(output, {copied, input.data() + input.size()});
}

/// Appends the spans of the pieces of `input` between the separators at
/// `found`, which are ascending byte ranges, with `base` added to every
/// offset. `max` bounds the number of splits; `reverse` keeps the rightmost
/// ones.
auto append_pieces(std::string_view input, nova::storage::Index base,
                   std::span<std::pair<size_t, size_t> const> found,
                   Option<int64_t> max, bool reverse,
                   std::vector<nova::storage::Span>& ranges) -> void {
  auto first = size_t{0};
  auto last = found.size();
  if (max and found.size() > static_cast<size_t>(*max)) {
    if (reverse) {
      first = found.size() - static_cast<size_t>(*max);
    } else {
      last = static_cast<size_t>(*max);
    }
  }
  auto pos = base;
  for (auto k = first; k < last; ++k) {
    auto const [begin, end] = found[k];
    ranges.emplace_back(pos, base + static_cast<nova::storage::Index>(begin));
    pos = base + static_cast<nova::storage::Index>(end);
  }
  ranges.emplace_back(pos,
                      base + static_cast<nova::storage::Index>(input.size()));
}

struct StartsEndsWithArgs {
  nova::ValueArgument x;
  nova::ValueArgument prefix;
  bool ignore_case = false;
  location call;
};

template <bool StartsWith>
class StartsEndsWithFunction final {
public:
  static auto eval(StartsEndsWithArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    return apply_kernel<2>(
      frame, (StartsWith ? "starts_with" : "ends_with"), {args.x, args.prefix},
      args.call,
      [ignore_case = args.ignore_case](diagnostic_handler&,
                                       std::string_view subject,
                                       std::string_view arg) -> Option<Bool> {
        if (ignore_case) {
          auto const subject_folded = unicode::utf8_fold_case(subject);
          auto const arg_folded = unicode::utf8_fold_case(arg);
          return StartsWith ? subject_folded.starts_with(arg_folded)
                            : subject_folded.ends_with(arg_folded);
        }
        return StartsWith ? subject.starts_with(arg) : subject.ends_with(arg);
      });
  }
};

template <bool StartsWith>
class starts_or_ends_with : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return StartsWith ? "starts_with" : "ends_with";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<StartsEndsWithArgs,
                                     StartsEndsWithFunction<StartsWith>>{};
    d.positional("x", &StartsEndsWithArgs::x, "string");
    d.positional("prefix", &StartsEndsWithArgs::prefix, "string");
    d.named_optional("ignore_case", &StartsEndsWithArgs::ignore_case);
    d.call_location(&StartsEndsWithArgs::call);
    return std::move(d).finish();
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
                               arg_expr = std::move(arg_expr), ignore_case](
                                evaluator eval, session ctx) -> series {
      TENZIR_UNUSED(ctx);
      auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
      check(b.Reserve(eval.length()));
      for (auto [subject, arg] :
           split_multi_series(eval(subject_expr), eval(arg_expr))) {
        TENZIR_ASSERT(subject.length() == arg.length());
        auto const* subject_array = try_as<arrow::StringArray>(&*subject.array);
        auto const* arg_array = try_as<arrow::StringArray>(&*arg.array);
        if (not subject_array or not arg_array) {
          // TODO: Emit a warning for non-null arrays.
          check(b.AppendNulls(arg.length()));
          continue;
        }
        auto subject_lc = std::shared_ptr<arrow::StringArray>{};
        auto arg_lc = std::shared_ptr<arrow::StringArray>{};
        if (ignore_case) {
          subject_lc = fold_case(*subject_array);
          arg_lc = fold_case(*arg_array);
          subject_array = subject_lc.get();
          arg_array = arg_lc.get();
        }
        for (auto i = int64_t{0}; i < subject_array->length(); ++i) {
          if (subject_array->IsNull(i) or arg_array->IsNull(i)) {
            check(b.AppendNull());
            continue;
          }
          auto result = bool{};
          if (StartsWith) {
            result = subject_array->Value(i).starts_with(arg_array->Value(i));
          } else {
            result = subject_array->Value(i).ends_with(arg_array->Value(i));
          }
          check(b.Append(result));
        }
      }
      return series{bool_type{}, finish(b)};
    });
  }
};

struct MatchRegexArgs {
  nova::ValueArgument x;
  located<std::string> pattern;
  /// Compiled from `pattern` in `validate`.
  std::shared_ptr<re2::RE2 const> regex;
  location call;
};

class MatchRegexFunction final {
public:
  static auto eval(MatchRegexArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    return apply_kernel<1>(
      frame, "match_regex", {args.x}, args.call,
      detail::overload{
        [](diagnostic_handler&, Null) -> Option<Bool> {
          return None{};
        },
        [&](diagnostic_handler&, std::string_view value) -> Option<Bool> {
          return re2::RE2::PartialMatch({value.data(), value.size()},
                                        *args.regex);
        },
      });
  }
};

class match_regex : public virtual function_plugin,
                    public virtual nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "match_regex";
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<MatchRegexArgs, MatchRegexFunction>{};
    d.positional("input", &MatchRegexArgs::x, "string");
    d.positional("regex", &MatchRegexArgs::pattern);
    d.call_location(&MatchRegexArgs::call);
    d.validate(
      [](MatchRegexArgs& args, diagnostic_handler& dh) -> failure_or<void> {
        TRY(args.regex, compile_regex(args.pattern, dh));
        return {};
      });
    return std::move(d).finish();
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

/// The code points that `trim` removes when it gets explicit characters.
struct TrimCharacters {
  std::bitset<128> ascii;
  std::vector<char32_t> other;

  auto contains(char32_t code_point) const -> bool {
    return code_point < 0x80 ? ascii.test(code_point)
                             : std::ranges::binary_search(other, code_point);
  }
};

enum class TrimSide { both, start, end };

struct TrimArgs {
  nova::ValueArgument x;
  Option<located<std::string>> characters;
  TrimCharacters trimmed;
  std::string name;
  TrimSide side = TrimSide::both;
  location call;
};

class TrimFunction final {
public:
  static auto eval(TrimArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    auto strings = string_rows(args, frame);
    if (not strings or not strings->present.any()) {
      return frame.null();
    }
    auto const& table = unicode::Table::get();
    auto const start = args.side != TrimSide::end;
    auto const end = args.side != TrimSide::start;
    auto trimmed = [&](char32_t c) {
      return args.characters ? args.trimmed.contains(c)
                             : table.properties(c).space;
    };
    auto const* dense
      = try_as<storage::DenseStringOffsetStorage>(strings->data.storage());
    if (not dense) {
      // Constants have no span-addressable owner. Trim once and retain a
      // constant result, copying only its surviving characters once.
      auto const kept
        = trim_code_points(*strings->data.get(0), start, end, trimmed);
      if (not kept) {
        diagnostic::warning("`{}` expected valid UTF-8", args.name)
          .primary(args.x.source)
          .emit(frame);
        return frame.null();
      }
      if (kept->size() > max_string_size) {
        diagnostic::warning("{} result exceeds maximum string array size",
                            args.name)
          .primary(args.call)
          .emit(frame);
        return frame.null();
      }
      auto result
        = Array<String>{storage::ConstantStorage<std::string, std::string_view>{
          strings->data.length(), std::string{*kept}}};
      return Array<Data>{std::move(result)}.null_where(
        frame.mask().and_not(strings->present));
    }
    auto ranges = storage::DataOwner<storage::Span[]>::make_uninitialized(
      strings->data.length());
    auto invalid = storage::BitMap::Mutable{strings->data.length()};
    auto warn_invalid = WarnOnce{};
    for (auto row : storage::bitmap_iteration(strings->present)) {
      if (not row) {
        ranges.emplace_back(0, 0);
        continue;
      }
      auto const input = *strings->data.get(*row);
      auto const kept = trim_code_points(input, start, end, trimmed);
      if (not kept) {
        invalid.set(*row, true);
        ranges.emplace_back(0, 0);
        warn_invalid(frame,
                     diagnostic::warning("`{}` expected valid UTF-8", args.name)
                       .primary(args.x.source));
        continue;
      }
      auto const base = dense->span(*row).begin;
      auto const begin
        = base + static_cast<storage::Index>(kept->data() - input.data());
      ranges.emplace_back(begin,
                          begin + static_cast<storage::Index>(kept->size()));
    }
    auto result = Array<String>{
      Type<String>::PrimaryPhysicalStorage{dense->data(), ranges.finish()}};
    return Array<Data>{std::move(result)}.null_where(
      frame.mask().and_not(strings->present) | std::move(invalid).finish());
  }
};

class trim : public virtual nova::FunctionPlugin {
public:
  trim(std::string name, std::string fn_name, TrimSide side)
    : name_{std::move(name)}, fn_name_{std::move(fn_name)}, side_{side} {
  }

  auto name() const -> std::string override {
    return name_;
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<TrimArgs, TrimFunction>{};
    d.positional("x", &TrimArgs::x, "string");
    d.positional("chars", &TrimArgs::characters);
    d.call_location(&TrimArgs::call);
    d.validate([name = name_, side = side_](
                 TrimArgs& args, diagnostic_handler& dh) -> failure_or<void> {
      args.name = name;
      args.side = side;
      if (not args.characters) {
        return {};
      }
      if (not valid_utf8(args.characters->inner)) {
        diagnostic::error("`{}` expected valid UTF-8 characters", name)
          .primary(*args.characters)
          .emit(dh);
        return failure::promise();
      }
      for_each_code_point(args.characters->inner, unicode::Table::get(),
                          [&](char32_t c, unicode::Properties) {
                            if (c < 0x80) {
                              args.trimmed.ascii.set(c);
                            } else {
                              args.trimmed.other.push_back(c);
                            }
                            return true;
                          });
      std::ranges::sort(args.trimmed.other);
      return {};
    });
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto characters = Option<std::string>{};
    TRY(argument_parser2::function(name())
          .positional("x", subject_expr, "string")
          .positional("chars", characters)
          .parse(inv, ctx));
    auto options = Option<arrow::compute::TrimOptions>{};
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
  TrimSide side_;
};

struct PadArgs {
  nova::ValueArgument x;
  nova::ValueArgument length;
  Option<located<std::string>> pad_char;
  std::string name;
  bool left = false;
  location call;
};

class PadFunction final {
public:
  static auto eval(PadArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    auto const pad_char = args.pad_char ? std::string_view{args.pad_char->inner}
                                        : std::string_view{" "};
    auto mask = accepted_rows<2>(frame, args.name, args.call,
                                 {args.x, args.length}, string_and_count);
    auto strings = args.x.data.get_alternative<String>();
    if (strings) {
      strings->present = strings->present & mask;
    }
    auto signed_lengths = args.length.data.get_alternative<Int>();
    auto unsigned_lengths = args.length.data.get_alternative<UInt>();
    auto warn_invalid = WarnOnce{};
    auto warn_too_large = WarnOnce{};
    return build_strings(
      std::move(strings), frame,
      [&](storage::Index row, std::string_view str, CharacterBuilder& output) {
        auto target = uint64_t{0};
        if (signed_lengths and signed_lengths->present.get(row)) {
          target = static_cast<uint64_t>(
            std::max(*signed_lengths->data.get(row), int64_t{0}));
        } else if (unsigned_lengths and unsigned_lengths->present.get(row)) {
          target = *unsigned_lengths->data.get(row);
        } else {
          return false;
        }
        if (not valid_utf8(str)) {
          warn_invalid(frame, diagnostic::warning("`{}` expected valid UTF-8",
                                                  args.name)
                                .primary(args.x.source));
          return false;
        }
        auto const str_length = unicode::utf8_codepoint_count(str);
        auto const padding = str_length < target ? target - str_length : 0;
        auto const remaining
          = max_string_size - static_cast<size_t>(output.size());
        if (str.size() > remaining
            or padding > (remaining - str.size()) / pad_char.size()) {
          warn_too_large(frame, diagnostic::warning("`{}` result exceeds "
                                                    "maximum string size",
                                                    args.name)
                                  .primary(args.length.source));
          return false;
        }
        output.reserve_at_least(output.size()
                                + static_cast<storage::Index>(
                                  str.size() + padding * pad_char.size()));
        if (not args.left) {
          output.move_append(str.begin(), str.end());
        }
        for (auto i = uint64_t{0}; i < padding; ++i) {
          output.move_append(pad_char.begin(), pad_char.end());
        }
        if (args.left) {
          output.move_append(str.begin(), str.end());
        }
        return true;
      });
  }
};

class pad : public virtual nova::FunctionPlugin {
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

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<PadArgs, PadFunction>{};
    d.positional("x", &PadArgs::x, "string");
    d.positional("length", &PadArgs::length, "int");
    d.positional("pad_char", &PadArgs::pad_char);
    d.call_location(&PadArgs::call);
    d.validate([name = name_, left = pad_left_](
                 PadArgs& args, diagnostic_handler& dh) -> failure_or<void> {
      args.name = name;
      args.left = left;
      if (args.pad_char) {
        if (not valid_utf8(args.pad_char->inner)) {
          diagnostic::error("`{}` expected valid UTF-8 for padding", name)
            .primary(*args.pad_char)
            .emit(dh);
          return failure::promise();
        }
        auto const length = unicode::utf8_codepoint_count(args.pad_char->inner);
        if (length != 1) {
          diagnostic::error("`{}` expected single character for padding, "
                            "but got `{}` with length {}",
                            name, args.pad_char->inner, length)
            .primary(*args.pad_char)
            .emit(dh);
          return failure::promise();
        }
      }
      return {};
    });
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto length_expr = ast::expression{};
    auto pad_char_arg = Option<located<std::string>>{};
    TRY(argument_parser2::function(name())
          .positional("x", subject_expr, "string")
          .positional("length", length_expr, "int")
          .positional("pad_char", pad_char_arg)
          .parse(inv, ctx));
    auto pad_char
      = pad_char_arg.value_or(located<std::string>{" ", location::unknown});
    // Validate pad character is single character
    if (pad_char_arg.has_value()) {
      auto const pad_char_length = detail::narrow<int64_t>(
        unicode::utf8_codepoint_count(pad_char.inner));
      if (pad_char_length != 1) {
        diagnostic::error("`{}` expected single character for padding, "
                          "but got `{}` with length {}",
                          name(), pad_char.inner, pad_char_length)
          .primary(pad_char)
          .emit(ctx);
        return failure::promise();
      }
    }
    return function_use::make([subject_expr = std::move(subject_expr),
                               length_expr = std::move(length_expr),
                               pad_char = std::move(pad_char),
                               pad_left = pad_left_, name = name_](
                                evaluator eval, session ctx) -> multi_series {
      auto b = arrow::StringBuilder{};
      for (auto [subject, length] :
           split_multi_series(eval(subject_expr), eval(length_expr))) {
        TENZIR_ASSERT(subject.length() == length.length());
        auto const* subject_array = try_as<arrow::StringArray>(&*subject.array);
        auto const* signed_length_array
          = try_as<arrow::Int64Array>(&*length.array);
        auto const* unsigned_length_array
          = try_as<arrow::UInt64Array>(&*length.array);
        auto const subject_is_null = is<arrow::NullArray>(*subject.array);
        auto const length_is_null = is<arrow::NullArray>(*length.array);
        if (not subject_array
            or (not signed_length_array and not unsigned_length_array)) {
          if (not subject_is_null and not subject_array) {
            diagnostic::warning("`{}` expected `string`, but got `{}`", name,
                                subject.type.kind())
              .primary(subject_expr)
              .emit(ctx);
          }
          if (not length_is_null and not signed_length_array
              and not unsigned_length_array) {
            diagnostic::warning("`{}` expected `int`, but got `{}`", name,
                                length.type.kind())
              .primary(length_expr)
              .emit(ctx);
          }
          check(b.AppendNulls(subject.length()));
          continue;
        }
        auto append = [&](auto const& length_array) {
          for (auto i = int64_t{0}; i < subject_array->length(); ++i) {
            if (subject_array->IsNull(i) or length_array.IsNull(i)) {
              check(b.AppendNull());
              continue;
            }
            auto str = subject_array->GetView(i);
            auto target_length = detail::narrow<int64_t>(length_array.Value(i));
            auto const str_length
              = detail::narrow<int64_t>(unicode::utf8_codepoint_count(str));
            if (str_length >= target_length) {
              // String is already long enough.
              check(b.Append(str));
              continue;
            }
            // Calculate padding needed.
            auto padding_needed
              = static_cast<size_t>(target_length - str_length);
            auto result = std::string{};
            result.reserve(str.size() + padding_needed * pad_char.inner.size());
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
        };
        if (signed_length_array) {
          append(*signed_length_array);
        } else {
          TENZIR_ASSERT(unsigned_length_array);
          append(*unsigned_length_array);
        }
      }

      return series{string_type{}, finish(b)};
    });
  }

private:
  std::string name_;
  bool pad_left_;
};

struct RepeatArgs {
  nova::ValueArgument x;
  nova::ValueArgument n;
  location call;
};

class RepeatFunction final {
public:
  static auto eval(RepeatArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    auto mask = accepted_rows<2>(frame, "repeat", args.call, {args.x, args.n},
                                 string_and_count);
    auto strings = args.x.data.get_alternative<String>();
    if (strings) {
      strings->present = strings->present & mask;
    }
    auto signed_counts = args.n.data.get_alternative<Int>();
    auto unsigned_counts = args.n.data.get_alternative<UInt>();
    auto warn_negative = WarnOnce{};
    auto warn_too_large = WarnOnce{};
    return build_strings(
      std::move(strings), frame,
      [&](storage::Index row, std::string_view str, CharacterBuilder& output) {
        auto n = uint64_t{0};
        if (signed_counts and signed_counts->present.get(row)) {
          auto const count = *signed_counts->data.get(row);
          if (count < 0) {
            warn_negative(frame,
                          diagnostic::warning("`repeat` expected non-negative "
                                              "count, but got {}",
                                              count)
                            .primary(args.n.source));
            return false;
          }
          n = static_cast<uint64_t>(count);
        } else if (unsigned_counts and unsigned_counts->present.get(row)) {
          n = *unsigned_counts->data.get(row);
        } else {
          return false;
        }
        if (n == 0 or str.empty()) {
          return true;
        }
        if (n > (max_string_size - static_cast<size_t>(output.size()))
                  / str.size()) {
          warn_too_large(frame, diagnostic::warning("`repeat` result exceeds "
                                                    "maximum string size")
                                  .primary(args.n.source));
          return false;
        }
        output.reserve_at_least(output.size()
                                + static_cast<storage::Index>(n * str.size()));
        for (auto i = uint64_t{0}; i < n; ++i) {
          output.move_append(str.begin(), str.end());
        }
        return true;
      });
  }
};

class repeat : public virtual nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "repeat_fn";
  }

  auto function_name() const -> std::string override {
    return "repeat";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<RepeatArgs, RepeatFunction>{};
    d.positional("x", &RepeatArgs::x, "string");
    d.positional("n", &RepeatArgs::n, "int");
    d.call_location(&RepeatArgs::call);
    return std::move(d).finish();
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
        auto const* subject_array = try_as<arrow::StringArray>(&*subject.array);
        auto const* signed_count_array
          = try_as<arrow::Int64Array>(&*count.array);
        auto const* unsigned_count_array
          = try_as<arrow::UInt64Array>(&*count.array);
        auto const subject_is_null = is<arrow::NullArray>(*subject.array);
        auto const count_is_null = is<arrow::NullArray>(*count.array);
        if (not subject_array
            or (not signed_count_array and not unsigned_count_array)) {
          if (not subject_is_null and not subject_array) {
            diagnostic::warning("`repeat` expected `string`, but got `{}`",
                                subject.type.kind())
              .primary(subject_expr)
              .emit(ctx);
          }
          if (not count_is_null and not signed_count_array
              and not unsigned_count_array) {
            diagnostic::warning("`repeat` expected `int`, but got `{}`",
                                count.type.kind())
              .primary(count_expr)
              .emit(ctx);
          }
          check(b.AppendNulls(subject.length()));
          continue;
        }
        auto append = [&](auto const& count_array) {
          for (auto i = int64_t{0}; i < subject_array->length(); ++i) {
            if (subject_array->IsNull(i) or count_array.IsNull(i)) {
              check(b.AppendNull());
              continue;
            }
            auto str = subject_array->GetView(i);
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
              diagnostic::warning("`repeat` result exceeds maximum string "
                                  "size")
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
        };
        if (signed_count_array) {
          append(*signed_count_array);
        } else {
          TENZIR_ASSERT(unsigned_count_array);
          append(*unsigned_count_array);
        }
      }
      return series{string_type{}, finish(b)};
    });
  }
};

enum class StringMethod {
  capitalize,
  to_lower,
  reverse,
  to_title,
  to_upper,
  is_alnum,
  is_alpha,
  is_lower,
  is_numeric,
  is_printable,
  is_title,
  is_upper,
  length_bytes,
  length_chars,
};

struct StringMethodArgs {
  nova::ValueArgument x;
  std::string name;
  StringMethod method = {};
  location call;
};

class StringMethodFunction final {
public:
  static auto eval(StringMethodArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using enum StringMethod;
    using unicode::Properties;
    auto const& table = unicode::Table::get();
    auto transform = [&](auto f) {
      return transform_strings(
        args, frame, [&](std::string_view input, CharacterBuilder& output) {
          return f(input, table, output);
        });
    };
    auto classify = [&](auto f) {
      return eval_utf8(args, frame,
                       [&](std::string_view input) -> Option<nova::Bool> {
                         auto const result = f(input);
                         if (not result) {
                           return None{};
                         }
                         return *result;
                       });
    };
    auto any = [](char32_t, Properties) {
      return true;
    };
    auto cased = [](char32_t, Properties p) {
      return p.cased;
    };
    switch (args.method) {
      case capitalize:
        return transform(map_case<CaseMapping::capitalize>);
      case to_lower:
        return transform(map_case<CaseMapping::lower>);
      case to_title:
        return transform(map_case<CaseMapping::title>);
      case to_upper:
        return transform(map_case<CaseMapping::upper>);
      case reverse:
        return transform_strings(args, frame, reverse_code_points);
      case is_alnum:
        return classify([&](std::string_view input) {
          return all_and_any(
            input, table,
            [](char32_t, Properties p) {
              return p.alpha or p.numeric;
            },
            any);
        });
      case is_alpha:
        return classify([&](std::string_view input) {
          return all_and_any(
            input, table,
            [](char32_t, Properties p) {
              return p.alpha;
            },
            any);
        });
      case is_lower:
        return classify([&](std::string_view input) {
          return all_and_any(
            input, table,
            [](char32_t, Properties p) {
              return not p.cased or p.lower;
            },
            cased);
        });
      case is_numeric:
        return classify([&](std::string_view input) {
          return all_and_any(
            input, table,
            [](char32_t, Properties p) {
              return p.numeric;
            },
            any);
        });
      case is_printable:
        return classify([&](std::string_view input) {
          if (input.empty()) {
            return Option<bool>{true};
          }
          return all_and_any(
            input, table,
            [](char32_t c, Properties p) {
              return c == U' ' or p.printable;
            },
            any);
        });
      case is_title:
        return classify([&](std::string_view input) {
          return is_titlecased(input, table);
        });
      case is_upper:
        return classify([&](std::string_view input) {
          return all_and_any(
            input, table,
            [](char32_t, Properties p) {
              return not p.cased or p.upper;
            },
            cased);
        });
      case length_bytes:
        return eval_strings(args, frame,
                            [](std::string_view input) -> Option<nova::Int> {
                              return static_cast<int64_t>(input.size());
                            });
      case length_chars:
        return eval_utf8(
          args, frame, [](std::string_view input) -> Option<nova::Int> {
            // Counting code points assumes valid UTF-8.
            if (not valid_utf8(input)) {
              return None{};
            }
            return static_cast<int64_t>(unicode::utf8_codepoint_count(input));
          });
    }
    TENZIR_UNREACHABLE();
  }
};

class nullary_method : public virtual nova::FunctionPlugin {
public:
  nullary_method(std::string name, std::string fn_name, type result_ty,
                 StringMethod method)
    : name_{std::move(name)},
      fn_name_{std::move(fn_name)},
      result_ty_{std::move(result_ty)},
      result_arrow_ty_{result_ty_.to_arrow_type()},
      method_{method} {
  }

  template <concrete_type Ty>
  nullary_method(std::string name, std::string fn_name, Ty result_ty,
                 StringMethod method)
    : nullary_method(std::move(name), std::move(fn_name),
                     type{std::move(result_ty)}, method) {
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

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<StringMethodArgs, StringMethodFunction>{};
    d.positional("x", &StringMethodArgs::x, "string");
    d.call_location(&StringMethodArgs::call);
    d.validate([name = function_name(),
                method = method_](StringMethodArgs& args,
                                  diagnostic_handler&) -> failure_or<void> {
      args.name = name;
      args.method = method;
      return {};
    });
    return std::move(d).finish();
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
  StringMethod method_;
};

struct ReplaceArgs {
  nova::ValueArgument x;
  nova::ValueArgument pattern;
  nova::ValueArgument replacement;
  Option<located<int64_t>> max;
  bool ignore_case = false;
  location call;
};

class ReplaceFunction final {
public:
  static auto eval(ReplaceArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    auto max = args.max ? args.max->inner : int64_t{-1};
    auto mask
      = accepted_rows<3>(frame, "replace", args.call,
                         {args.x, args.pattern, args.replacement},
                         [](auto const& tags) {
                           return std::ranges::all_of(tags, string_or_null);
                         });
    auto strings = args.x.data.get_alternative<String>();
    if (strings) {
      strings->present = strings->present & mask;
    }
    auto patterns = args.pattern.data.get_alternative<String>();
    auto replacements = args.replacement.data.get_alternative<String>();
    auto warn_too_large = WarnOnce{};
    return build_strings(
      std::move(strings), frame,
      [&](storage::Index row, std::string_view subject,
          CharacterBuilder& output) {
        // A null pattern matches nothing; a null replacement deletes matches.
        auto const pattern = patterns and patterns->present.get(row)
                               ? *patterns->data.get(row)
                               : std::string_view{};
        auto const replacement = replacements and replacements->present.get(row)
                                   ? *replacements->data.get(row)
                                   : std::string_view{};
        auto const fits = append_replacements(subject, pattern, replacement,
                                              max, args.ignore_case, output);
        if (not fits) {
          warn_too_large(frame, diagnostic::warning("`replace` result exceeds "
                                                    "maximum string size")
                                  .primary(args.x.source));
        }
        return fits;
      });
  }
};

struct ReplaceRegexArgs {
  nova::ValueArgument x;
  located<std::string> pattern;
  located<std::string> replacement;
  Option<located<int64_t>> max;
  /// Compiled from `pattern` in `validate`.
  std::shared_ptr<re2::RE2 const> regex;
  std::string name = "replace_regex";
  location call;
};

class ReplaceRegexFunction final {
public:
  static auto eval(ReplaceRegexArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto max = Option<int64_t>{};
    if (args.max) {
      max = args.max->inner;
    }
    return transform_strings(
      args, frame,
      [&](std::string_view input, CharacterBuilder& output) -> Option<bool> {
        return replace_matches(input, *args.regex, args.replacement.inner, max,
                               output);
      });
  }
};

class replace : public nova::FunctionPlugin {
public:
  replace() = default;
  explicit replace(bool regex) : regex_{regex} {
  }

  auto name() const -> std::string override {
    return regex_ ? "replace_regex" : "replace_fn";
  }

  auto function_name() const -> std::string override {
    return regex_ ? "replace_regex" : "replace";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    if (regex_) {
      auto d
        = nova::FunctionDescriber<ReplaceRegexArgs, ReplaceRegexFunction>{};
      d.positional("x", &ReplaceRegexArgs::x, "string");
      d.positional("pattern", &ReplaceRegexArgs::pattern);
      d.positional("replacement", &ReplaceRegexArgs::replacement);
      d.named("max", &ReplaceRegexArgs::max);
      d.call_location(&ReplaceRegexArgs::call);
      d.validate(
        [](ReplaceRegexArgs& args, diagnostic_handler& dh) -> failure_or<void> {
          TRY(validate_max_replacements(args.max, dh));
          TRY(args.regex, compile_regex(args.pattern, dh));
          auto error = std::string{};
          if (not args.regex->CheckRewriteString(args.replacement.inner,
                                                 &error)) {
            diagnostic::error("invalid replacement: {}", error)
              .primary(args.replacement)
              .emit(dh);
            return failure::promise();
          }
          return {};
        });
      return std::move(d).finish();
    }
    auto d = nova::FunctionDescriber<ReplaceArgs, ReplaceFunction>{};
    d.positional("x", &ReplaceArgs::x, "string");
    d.positional("pattern", &ReplaceArgs::pattern, "string");
    d.positional("replacement", &ReplaceArgs::replacement, "string");
    d.named("max", &ReplaceArgs::max);
    d.named_optional("ignore_case", &ReplaceArgs::ignore_case);
    d.call_location(&ReplaceArgs::call);
    d.validate([](ReplaceArgs& args, diagnostic_handler& dh) {
      return validate_max_replacements(args.max, dh);
    });
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto pattern_expr = ast::expression{};
    auto replacement_expr = ast::expression{};
    auto max_replacements = Option<located<int64_t>>{};
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
      TRY(validate_max_replacements(max_replacements, ctx));
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
    TRY(validate_max_replacements(max_replacements, ctx));
    auto literal_pattern = literal_string_arg(pattern_expr);
    auto literal_replacement = literal_string_arg(replacement_expr);
    return function_use::make(
      [this, subject_expr = std::move(subject_expr),
       pattern_expr = std::move(pattern_expr),
       replacement_expr = std::move(replacement_expr),
       literal_pattern = std::move(literal_pattern),
       literal_replacement = std::move(literal_replacement), max_replacements,
       ignore_case](evaluator eval, session ctx) -> multi_series {
        auto result_type = string_type{};
        auto max = max_replacements ? max_replacements->inner : -1;
        if (literal_pattern and literal_replacement) {
          if (not ignore_case and not literal_pattern->empty()) {
            return replace_substring(eval(subject_expr), "replace_substring",
                                     *literal_pattern, *literal_replacement,
                                     max, pattern_expr.get_location(),
                                     subject_expr, name(), ctx);
          }
          return map_series(eval(subject_expr), [&](series subject) {
            auto f = detail::overload{
              [&](arrow::StringArray const& array) {
                auto folded_pattern
                  = literal_pattern->empty()
                      ? std::string{}
                      : unicode::utf8_fold_case(*literal_pattern);
                auto b = arrow::StringBuilder{tenzir::arrow_memory_pool()};
                check(b.Reserve(array.length()));
                auto total_size = size_t{0};
                for (auto i = int64_t{0}; i < array.length(); ++i) {
                  if (array.IsNull(i)) {
                    check(b.AppendNull());
                    continue;
                  }
                  auto result = literal_pattern->empty()
                                  ? Option{std::string{array.Value(i)}}
                                  : replace_literal_ignore_case(
                                      array.Value(i), folded_pattern,
                                      *literal_replacement, max,
                                      max_string_size - total_size);
                  if (not result
                      or result->size() > max_string_size - total_size) {
                    diagnostic::warning(
                      "`replace` result exceeds maximum string array size")
                      .primary(subject_expr)
                      .emit(ctx);
                    check(b.AppendNull());
                    continue;
                  }
                  check(b.Append(*result));
                  total_size += result->size();
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
        auto total_size = size_t{0};
        for (auto [subject, pattern, replacement] :
             split_multi_series(std::move(subject), std::move(pattern),
                                std::move(replacement))) {
          TENZIR_ASSERT(subject.length() == pattern.length());
          TENZIR_ASSERT(subject.length() == replacement.length());
          auto append_replaced = [&](std::string_view subject_value,
                                     std::string_view pattern_value,
                                     std::string_view replacement_value) {
            auto result
              = replace_literal(subject_value, pattern_value, replacement_value,
                                max, ignore_case, max_string_size - total_size);
            if (not result) {
              diagnostic::warning(
                "`replace` result exceeds maximum string array size")
                .primary(subject_expr)
                .emit(ctx);
              check(b.AppendNull());
              return;
            }
            check(b.Append(*result));
            total_size += result->size();
          };
          auto const* subject_array
            = try_as<arrow::StringArray>(&*subject.array);
          auto const* pattern_array
            = try_as<arrow::StringArray>(&*pattern.array);
          auto const* replacement_array
            = try_as<arrow::StringArray>(&*replacement.array);
          auto const subject_is_null = is<arrow::NullArray>(*subject.array);
          auto const pattern_is_null = is<arrow::NullArray>(*pattern.array);
          auto const replacement_is_null
            = is<arrow::NullArray>(*replacement.array);
          if ((not subject_array and not subject_is_null)
              or (not pattern_array and not pattern_is_null)
              or (not replacement_array and not replacement_is_null)) {
            if (not warned) {
              warned = true;
              diagnostic::warning("`replace` expected `string`, but got `{}`, "
                                  "`{}`, and `{}`",
                                  subject.type.kind(), pattern.type.kind(),
                                  replacement.type.kind())
                .primary(subject_expr)
                .primary(pattern_expr)
                .primary(replacement_expr)
                .emit(ctx);
            }
            check(b.AppendNulls(subject.length()));
            continue;
          }
          if (subject_is_null) {
            check(b.AppendNulls(subject.length()));
            continue;
          }
          TENZIR_ASSERT(subject_array);
          for (auto i = int64_t{0}; i < subject_array->length(); ++i) {
            if (subject_array->IsNull(i)) {
              check(b.AppendNull());
              continue;
            }
            auto pattern_value = std::string_view{};
            if (pattern_array and not pattern_array->IsNull(i)) {
              pattern_value = pattern_array->Value(i);
            }
            auto replacement_value = std::string_view{};
            if (pattern_array and replacement_array
                and not replacement_array->IsNull(i)) {
              replacement_value = replacement_array->Value(i);
            }
            append_replaced(subject_array->Value(i), pattern_value,
                            replacement_value);
          }
        }
        return series{result_type, finish(b)};
      });
  }

private:
  bool regex_ = {};
};

struct StringArgs {
  nova::ValueArgument x;
  location call;
};

template <bool Deprecated>
class StringFunction final {
public:
  static auto eval(StringArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    if constexpr (Deprecated) {
      diagnostic::warning("`str` has been renamed to `string`")
        .note("`str` alias will be removed and become a hard error in a future "
              "release")
        .primary(args.call)
        .emit(frame);
    }
    auto subject = args.x;
    auto result = stringify(subject.data, frame.mask(), args.x.source, frame);
    return Array<Data>{std::move(result.data)}.null_where(
      frame.mask().and_not(result.present));
  }
};

template <bool Deprecated>
class string_fn : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return Deprecated ? "str" : "string";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<StringArgs, StringFunction<Deprecated>>{};
    d.positional("x", &StringArgs::x, "any");
    d.call_location(&StringArgs::call);
    return std::move(d).finish();
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

struct SplitArgs {
  nova::ValueArgument x;
  located<std::string> pattern;
  Option<located<int64_t>> max;
  Option<location> reverse;
  bool ignore_case = false;
  location call;
};

/// The spans that a splitting function appends the pieces of one row to.
using PieceRanges = std::vector<nova::storage::Span>;

/// Splits the string rows of `x` into lists for the function `name`.
/// `split_into(v, base, ranges)` appends the pieces of `v` as spans offset by
/// `base`. The pieces point into the bytes of the input, so splitting copies no
/// string data.
template <class SplitInto>
auto split_strings(std::string_view name, nova::ValueArgument const& x,
                   nova::EvalFrame frame, SplitInto split_into)
  -> nova::Array<nova::Data> {
  using namespace nova;
  auto present = frame.mask();
  auto arg = x;
  // Resolve the subject to a concrete `Array<String>` plus the mask of
  // rows that are actually strings: either the whole column already is
  // one, or it's a `UnionArray` with a `String` alternative among others.
  // Nulls propagate silently; any other row is reported and excluded.
  auto mask = present;
  auto subject = Option<Array<String>>{};
  match(
    arg.data,
    [&](Array<String> s) {
      subject = std::move(s);
    },
    [&](const Array<Null>&) {},
    [&](const UnionArray& u) {
      auto strings = u.get_alternative<String>();
      auto const string_present = strings
                                    ? mask & strings->present
                                    : storage::BitMap{mask.length(), false};
      if (mask.and_not(string_present)
            .and_not(u.alternative_mask<Null>())
            .any()) {
        diagnostic::warning("expected `string`, got a different type")
          .primary(x.source)
          .emit(frame);
      }
      mask = string_present;
      if (strings) {
        subject = std::move(strings->data);
      }
    },
    [&](const auto&) {
      diagnostic::warning("expected `string`, got a different type")
        .primary(x.source)
        .emit(frame);
    });
  if (not subject) {
    return frame.null();
  }
  auto const length = subject->length();
  // The pieces are spans into the input's buffer, or into one copy of a
  // constant's value that all rows share. A constant splits only once.
  auto data = storage::DataOwner<char[]>{};
  auto const* dense
    = static_cast<storage::DenseStringOffsetStorage const*>(nullptr);
  auto row_pieces = PieceRanges{};
  match(
    subject->storage(),
    [&](storage::DenseStringOffsetStorage const& storage) {
      data = storage.data();
      dense = &storage;
    },
    [&](storage::ConstantStorage<std::string, std::string_view> const&) {
      auto const v = *subject->get(0);
      auto builder = storage::DataOwner<char[]>::Builder{};
      builder.move_append(v.begin(), v.end());
      data = builder.finish();
      split_into(v, storage::Index{0}, row_pieces);
    });
  // Each row gets its own pieces, so that the lists do not overlap, which
  // `merge_lists` requires. Shared bytes still count once per row, as a copy
  // of the pieces, such as for Arrow, must fit into 32-bit offsets too. The
  // rows beyond that become null.
  constexpr auto max_pieces
    = static_cast<size_t>(std::numeric_limits<storage::Index>::max());
  auto row_bytes = [&] {
    auto result = size_t{0};
    for (auto const& piece : row_pieces) {
      result += static_cast<size_t>(piece.end - piece.begin);
    }
    return result;
  };
  auto constant_bytes = dense ? size_t{0} : row_bytes();
  auto bytes = size_t{0};
  auto pieces = storage::DataOwner<storage::Span[]>::Builder{};
  auto spans = storage::DataOwner<storage::Span[]>::Builder{};
  auto too_large = storage::BitMap::Mutable{length};
  auto begin = storage::Index{0};
  for (auto i = storage::Index{0}; i < length; ++i) {
    if (mask.get(i)) {
      if (dense) {
        row_pieces.clear();
        split_into(*subject->get(i), dense->span(i).begin, row_pieces);
      }
      auto const size = dense ? row_bytes() : constant_bytes;
      if (row_pieces.size() <= max_pieces - static_cast<size_t>(pieces.size())
          and size <= max_string_size - bytes) {
        pieces.move_append(row_pieces.begin(), row_pieces.end());
        bytes += size;
      } else {
        too_large.set(i, true);
      }
    }
    auto const end = pieces.size();
    spans.emplace_back(begin, end);
    begin = end;
  }
  auto dropped = std::move(too_large).finish();
  if (dropped.any()) {
    diagnostic::warning("`{}` result exceeds maximum string array size", name)
      .primary(x.source)
      .emit(frame);
  }
  auto strings = Array<String>{
    storage::DenseStringOffsetStorage{std::move(data), pieces.finish()}};
  auto result = Array<List>{spans.finish(), Array<Data>{std::move(strings)}};
  // Non-string rows never got pieces written, so they become nulls.
  return Array<Data>{std::move(result)}.null_where(present.and_not(mask)
                                                   | dropped);
}

class SplitFunction final {
public:
  static auto eval(SplitArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    auto const max
      = args.max ? Option<int64_t>{args.max->inner} : Option<int64_t>{};
    auto const folded = args.ignore_case
                          ? unicode::utf8_fold_case(args.pattern.inner)
                          : std::string{};
    return split_strings(
      "split", args.x, frame,
      [&](std::string_view v, storage::Index base, PieceRanges& ranges) {
        if (args.ignore_case and not args.pattern.inner.empty()) {
          auto const found = unicode::utf8_fold_case_find(v, folded);
          append_pieces(v, base, found, max, args.reverse.has_value(), ranges);
          return;
        }
        if (args.pattern.inner.empty()) {
          ranges.emplace_back(base,
                              base + static_cast<storage::Index>(v.size()));
          return;
        }
        // `max`+`reverse` together need the total match count up front, to
        // know how many leading matches to skip; every other combination can
        // emit each piece as soon as its closing match is found.
        auto skip = size_t{0};
        if (max and args.reverse.has_value()) {
          auto total = size_t{0};
          for (auto pos = size_t{0};;) {
            auto const match = v.find(args.pattern.inner, pos);
            if (match == std::string_view::npos) {
              break;
            }
            ++total;
            pos = match + args.pattern.inner.size();
          }
          auto const count = static_cast<size_t>(*max);
          skip = total > count ? total - count : 0;
        }
        auto const limit = max ? static_cast<size_t>(*max) : SIZE_MAX;
        auto piece_begin = size_t{0};
        auto pos = size_t{0};
        auto emitted = size_t{0};
        while (emitted < limit) {
          auto const match = v.find(args.pattern.inner, pos);
          if (match == std::string_view::npos) {
            break;
          }
          pos = match + args.pattern.inner.size();
          if (skip > 0) {
            --skip;
            continue;
          }
          ranges.emplace_back(base + static_cast<storage::Index>(piece_begin),
                              base + static_cast<storage::Index>(match));
          piece_begin = pos;
          ++emitted;
        }
        ranges.emplace_back(base + static_cast<storage::Index>(piece_begin),
                            base + static_cast<storage::Index>(v.size()));
      });
  }
};

struct SplitRegexArgs {
  nova::ValueArgument x;
  located<std::string> pattern;
  Option<located<int64_t>> max;
  Option<location> reverse;
  /// Compiled from `pattern` in `validate`.
  std::shared_ptr<re2::RE2 const> regex;
  location call;
};

class SplitRegexFunction final {
public:
  static auto eval(SplitRegexArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto max = Option<int64_t>{};
    if (args.max) {
      max = args.max->inner;
    }
    auto const reverse = args.reverse.has_value();
    auto found = std::vector<std::pair<size_t, size_t>>{};
    auto match = std::array<absl::string_view, 1>{};
    return split_strings(
      "split_regex", args.x, frame,
      [&](std::string_view input, nova::storage::Index base,
          PieceRanges& ranges) {
        found.clear();
        if (max and *max == 0) {
          ranges.emplace_back(
            base, base + static_cast<nova::storage::Index>(input.size()));
          return;
        }
        for_each_match(
          input, *args.regex, match,
          [&](std::span<absl::string_view const> matches) {
            auto const begin
              = static_cast<size_t>(matches[0].data() - input.data());
            found.emplace_back(begin, begin + matches[0].size());
            if (not max) {
              return true;
            }
            // Without `reverse`, the first `max` separators suffice. With it,
            // only the last `max` matter, so drop older ones as they pile up.
            auto const count = static_cast<size_t>(*max);
            if (not reverse) {
              return found.size() < count;
            }
            if (found.size() == 2 * count) {
              found.erase(found.begin(),
                          found.begin() + static_cast<std::ptrdiff_t>(count));
            }
            return true;
          });
        append_pieces(input, base, found, max, reverse, ranges);
      });
  }
};

class split_fn : public nova::FunctionPlugin {
public:
  split_fn() = default;
  explicit split_fn(bool regex) : regex_{regex} {
  }

  auto name() const -> std::string override {
    return regex_ ? "split_regex" : "split";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto validate_max = [](Option<located<int64_t>> const& max,
                           diagnostic_handler& dh) -> failure_or<void> {
      if (max and max->inner < 0) {
        diagnostic::error("`max` must be at least 0, but got {}", max->inner)
          .primary(*max)
          .emit(dh);
        return failure::promise();
      }
      return {};
    };
    if (regex_) {
      auto d = nova::FunctionDescriber<SplitRegexArgs, SplitRegexFunction>{};
      d.positional("x", &SplitRegexArgs::x, "string");
      d.positional("pattern", &SplitRegexArgs::pattern);
      d.named("max", &SplitRegexArgs::max);
      d.named("reverse", &SplitRegexArgs::reverse);
      d.call_location(&SplitRegexArgs::call);
      d.validate([validate_max](SplitRegexArgs& args,
                                diagnostic_handler& dh) -> failure_or<void> {
        TRY(validate_max(args.max, dh));
        TRY(args.regex, compile_regex(args.pattern, dh));
        return {};
      });
      return std::move(d).finish();
    }
    auto d = nova::FunctionDescriber<SplitArgs, SplitFunction>{};
    d.positional("x", &SplitArgs::x, "string");
    d.positional("pattern", &SplitArgs::pattern);
    d.named("max", &SplitArgs::max);
    d.named("reverse", &SplitArgs::reverse);
    d.named_optional("ignore_case", &SplitArgs::ignore_case);
    d.call_location(&SplitArgs::call);
    d.validate([validate_max](SplitArgs& args, diagnostic_handler& dh) {
      return validate_max(args.max, dh);
    });
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto pattern = located<std::string>{};
    auto reverse = Option<location>{};
    auto max_splits = Option<located<int64_t>>{};
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
              auto fp = unicode::utf8_fold_case(pattern.inner);
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
                auto ranges = unicode::utf8_fold_case_find(v, fp);
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

struct JoinArgs {
  nova::ValueArgument x;
  Option<located<std::string>> separator;
  location call;
};

class JoinFunction final {
public:
  static auto eval(JoinArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    auto const separator = args.separator
                             ? std::string_view{args.separator->inner}
                             : std::string_view{};
    auto mask = accepted_rows<1>(
      frame, "join", args.call, {args.x}, [](auto const& tags) {
        return tags[0] == data_type_list::unique_index_of<List>
               or tags[0] == data_type_list::unique_index_of<Null>;
      });
    auto lists = args.x.data.get_alternative<List>();
    if (lists) {
      lists->present = lists->present & mask;
    }
    auto warn_null = WarnOnce{};
    auto warn_type = WarnOnce{};
    auto warn_too_large = WarnOnce{};
    return build_strings(
      std::move(lists), frame,
      [&](storage::Index, RowView<List> const& list, CharacterBuilder& output) {
        auto first = true;
        for (auto element : list) {
          auto ok = match(
            element,
            [&](RowView<String> value) {
              auto const fits = (std::exchange(first, false)
                                 or append_characters(output, separator))
                                and append_characters(output, *value);
              if (not fits) {
                warn_too_large(frame,
                               diagnostic::warning("join result exceeds "
                                                   "maximum string array size")
                                 .primary(args.call));
              }
              return fits;
            },
            [&](RowView<Null>) {
              warn_null(
                frame,
                diagnostic::warning("found `null` in list passed to `join`")
                  .primary(args.x.source)
                  .hint("consider using `.where(x => x != null)` before"));
              return false;
            },
            [&](auto const&) {
              warn_type(frame, diagnostic::warning("`join` expected "
                                                   "`list<string>`, but got a "
                                                   "list with other elements")
                                 .primary(args.x.source));
              return false;
            });
          if (not ok) {
            return false;
          }
        }
        return true;
      });
  }
};

class join : public virtual function_plugin,
             public virtual nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "join";
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<JoinArgs, JoinFunction>{};
    d.positional("x", &JoinArgs::x, "list");
    d.positional("separator", &JoinArgs::separator);
    d.call_location(&JoinArgs::call);
    return std::move(d).finish();
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    // TODO: Technically, this could be an expression and not just a constant
    // string.
    auto separator = Option<located<std::string>>{};
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

struct EqualsArgs {
  nova::ValueArgument x;
  nova::ValueArgument y;
  bool ignore_case = false;
  location call;
};

class EqualsFunction final {
public:
  static auto eval(EqualsArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    // Three-valued equality consistent with the `==` operator: two nulls
    // compare equal, a null and a string compare unequal.
    return apply_kernel<2>(
      frame, "equals", {args.x, args.y}, args.call,
      detail::overload{
        [&](diagnostic_handler&, std::string_view x,
            std::string_view y) -> Option<Bool> {
          if (args.ignore_case) {
            return unicode::utf8_fold_case(x) == unicode::utf8_fold_case(y);
          }
          return x == y;
        },
        []<class X, class Y>(diagnostic_handler&, X, Y) -> Option<Bool>
          requires(StringOrNull<X> and StringOrNull<Y>
                   and (std::same_as<X, Null> or std::same_as<Y, Null>))
        {
          return std::same_as<X, Y>;
        },
        });
  }
};

class equals : public virtual nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "equals";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<EqualsArgs, EqualsFunction>{};
    d.positional("x", &EqualsArgs::x, "string");
    d.positional("y", &EqualsArgs::y, "string");
    d.named_optional("ignore_case", &EqualsArgs::ignore_case);
    d.call_location(&EqualsArgs::call);
    return std::move(d).finish();
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
          auto const* left_array = try_as<arrow::StringArray>(&*left.array);
          auto const* right_array = try_as<arrow::StringArray>(&*right.array);
          auto const left_is_null = is<arrow::NullArray>(*left.array);
          auto const right_is_null = is<arrow::NullArray>(*right.array);
          if ((not left_array and not left_is_null)
              or (not right_array and not right_is_null)) {
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
            continue;
          }
          if (left_is_null and right_is_null) {
            check(b.AppendValues(left.length(), true));
            continue;
          }
          if (left_array and right_is_null) {
            for (auto i = int64_t{0}; i < left_array->length(); ++i) {
              check(b.Append(left_array->IsNull(i)));
            }
            continue;
          }
          if (left_is_null and right_array) {
            for (auto i = int64_t{0}; i < right_array->length(); ++i) {
              check(b.Append(right_array->IsNull(i)));
            }
            continue;
          }
          TENZIR_ASSERT(left_array);
          TENZIR_ASSERT(right_array);
          auto left_lc = std::shared_ptr<arrow::StringArray>{};
          auto right_lc = std::shared_ptr<arrow::StringArray>{};
          auto const* left_values = left_array;
          auto const* right_values = right_array;
          if (ignore_case) {
            left_lc = fold_case(*left_array);
            right_lc = fold_case(*right_array);
            left_values = left_lc.get();
            right_values = right_lc.get();
          }
          append_null_aware(*left_array, *right_array, [&](int64_t i) {
            return left_values->Value(i) == right_values->Value(i);
          });
        }
        return series{bool_type{}, finish(b)};
      });
  }
};

} // namespace

} // namespace tenzir::plugins::string

using namespace tenzir;
using namespace tenzir::plugins::string;

TENZIR_REGISTER_PLUGIN(starts_or_ends_with<true>)
TENZIR_REGISTER_PLUGIN(starts_or_ends_with<false>)

TENZIR_REGISTER_PLUGIN(match_regex)

TENZIR_REGISTER_PLUGIN(trim{"trim", "utf8_trim", TrimSide::both})
TENZIR_REGISTER_PLUGIN(trim{"trim_start", "utf8_ltrim", TrimSide::start})
TENZIR_REGISTER_PLUGIN(trim{"trim_end", "utf8_rtrim", TrimSide::end})

TENZIR_REGISTER_PLUGIN(pad{"pad_start", true})
TENZIR_REGISTER_PLUGIN(pad{"pad_end", false})
TENZIR_REGISTER_PLUGIN(repeat)

TENZIR_REGISTER_PLUGIN(nullary_method{"capitalize", "utf8_capitalize",
                                      string_type{}, StringMethod::capitalize})
TENZIR_REGISTER_PLUGIN(nullary_method{"to_lower", "utf8_lower", string_type{},
                                      StringMethod::to_lower})
TENZIR_REGISTER_PLUGIN(nullary_method{"reverse()", "utf8_reverse",
                                      string_type{}, StringMethod::reverse})
TENZIR_REGISTER_PLUGIN(nullary_method{"to_title", "utf8_title", string_type{},
                                      StringMethod::to_title})
TENZIR_REGISTER_PLUGIN(nullary_method{"to_upper", "utf8_upper", string_type{},
                                      StringMethod::to_upper})

TENZIR_REGISTER_PLUGIN(nullary_method{"is_alnum", "utf8_is_alnum", bool_type{},
                                      StringMethod::is_alnum})
TENZIR_REGISTER_PLUGIN(nullary_method{"is_alpha", "utf8_is_alpha", bool_type{},
                                      StringMethod::is_alpha})
TENZIR_REGISTER_PLUGIN(nullary_method{"is_lower", "utf8_is_lower", bool_type{},
                                      StringMethod::is_lower})
TENZIR_REGISTER_PLUGIN(nullary_method{"is_numeric", "utf8_is_numeric",
                                      bool_type{}, StringMethod::is_numeric})
TENZIR_REGISTER_PLUGIN(nullary_method{"is_printable", "utf8_is_printable",
                                      bool_type{}, StringMethod::is_printable})
TENZIR_REGISTER_PLUGIN(nullary_method{"is_title", "utf8_is_title", bool_type{},
                                      StringMethod::is_title})
TENZIR_REGISTER_PLUGIN(nullary_method{"is_upper", "utf8_is_upper", bool_type{},
                                      StringMethod::is_upper})

TENZIR_REGISTER_PLUGIN(nullary_method{
  "length_bytes", "binary_length", int64_type{}, StringMethod::length_bytes});
TENZIR_REGISTER_PLUGIN(nullary_method{
  "length_chars", "utf8_length", int64_type{}, StringMethod::length_chars});

TENZIR_REGISTER_PLUGIN(replace{true});
TENZIR_REGISTER_PLUGIN(replace{false});
TENZIR_REGISTER_PLUGIN(string_fn<false>);
TENZIR_REGISTER_PLUGIN(string_fn<true>);

TENZIR_REGISTER_PLUGIN(split_fn{true});
TENZIR_REGISTER_PLUGIN(split_fn{false});
TENZIR_REGISTER_PLUGIN(join);
TENZIR_REGISTER_PLUGIN(equals);
