//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <algorithm>
#include <iterator>
#include <string>
#include <type_traits>
#include <vector>

namespace tenzir::detail {

/// Unscapes a string according to an escaper.
/// @param str The string to escape.
/// @param escaper The escaper to use.
/// @returns The escaped version of *str*.
template <class Escaper>
std::string escape(std::string_view str, Escaper escaper) {
  std::string result;
  result.reserve(str.size());
  auto f = str.begin();
  auto l = str.end();
  auto out = std::back_inserter(result);
  while (f != l)
    escaper(f, out);
  return result;
}

/// Unscapes a string according to an unescaper.
/// @param str The string to unescape.
/// @param unescaper The unescaper to use.
/// @returns The unescaped version of *str*.
template <class Unescaper>
std::string unescape(std::string_view str, Unescaper unescaper) {
  std::string result;
  result.reserve(str.size());
  auto f = str.begin();
  auto l = str.end();
  auto out = std::back_inserter(result);
  while (f != l)
    if (!unescaper(f, l, out))
      return {};
  return result;
}

/// Escapes all non-printable characters in a string with `\xAA` where `AA` is
/// the byte in hexadecimal representation.
/// @param str The string to escape.
/// @returns The escaped string of *str*.
/// @relates bytes_escape_all byte_unescape
std::string byte_escape(std::string_view str);

/// Escapes all non-printable characters in a string with `\xAA` where `AA` is
/// the byte in hexadecimal representation, plus a given list of extra
/// characters to escape.
/// @param str The string to escape.
/// @param extra The extra characters to escape.
/// @returns The escaped string of *str*.
/// @relates bytes_escape_all byte_unescape
std::string byte_escape(std::string_view str, const std::string& extra);

/// Escapes all characters in a string with `\xAA` where `AA` is
/// the byte in hexadecimal representation of the character.
/// @param str The string to escape.
/// @returns The escaped string of *str*.
/// @relates byte_unescape
std::string byte_escape_all(std::string_view str);

/// Unescapes a byte-escaped string, i.e., replaces all occurrences of `\xAA`
/// with the value of the byte `AA`.
/// @param str The string to unescape.
/// @returns The unescaped string of *str*.
/// @relates byte_escape bytes_escape_all
std::string byte_unescape(std::string_view str);

/// Escapes a string by splitting all singular control characters into two
/// chars, e.g., the character '\n' becomes a two-character string "\n".
/// @param str The string to escape.
/// @returns The escaped string.
std::string control_char_escape(std::string_view str);

/// Escapes a string according to JSON escaping.
/// @param str The string to escape.
/// @returns The escaped string.
/// @relates json_unescape
std::string json_escape(std::string_view str);

/// Unescapes a string escaped with JSON escaping.
/// @param str The string to unescape.
/// @returns The unescaped string.
/// @relates json_escape
std::string json_unescape(std::string_view str);

/// Escapes a string according to percent-encoding.
/// @note This function escapes all non-*unreserved* characters as listed in
///       RFC3986. It does *not* correctly preserve HTTP URLs, but servers
///       merely as poor-man's substitute to prevent illegal characters from
///       slipping in.
/// @param str The string to escape.
/// @returns The escaped string.
/// @relates percent_unescape
std::string percent_escape(std::string_view str);

/// Unescapes a percent-encoded string.
/// @param str The string to unescape.
/// @returns The unescaped string.
/// @relates percent_escape
std::string percent_unescape(std::string_view str);

/// Escapes a string by repeating characters from a special set.
/// @param str The string to escape.
/// @param esc The set of characters to double-escape.
/// @returns The escaped string.
/// @relates double_unescape
std::string double_escape(std::string_view str, std::string_view esc);

/// Unescapes a string by removing consecutive character sequences.
/// @param str The string to unescape.
/// @param esc The set of repeated characters to unescape.
/// @returns The unescaped string.
/// @relates double_escape
std::string double_unescape(std::string_view str, std::string_view esc);

/// Replaces all occurences of a substring.
/// @param str The string in which to replace a substring.
/// @param search The string to search.
/// @param replace The replacement string.
/// @returns The string with replacements.
std::string
replace_all(std::string str, std::string_view search, std::string_view replace);

/// Splits a character sequence into a vector of substrings.
/// @param str The string to split.
/// @param sep The separator where to split.
/// @param max_splits The maximum number of splits to perform.
/// @pre `!sep.empty()`
/// @warning The lifetime of the returned substrings are bound to the lifetime
/// of the string pointed to by `str`.
/// @returns A vector of substrings.
std::vector<std::string_view>
split(std::string_view str, std::string_view sep, size_t max_splits = -1);

/// Splits a character sequence into two substrings.
/// If `sep` does not occur in `str`, the second substring will be empty.
/// @param str The string to split.
/// @param sep The separator where to split.
/// @pre `!sep.empty()`
/// @warning The lifetime of the returned substrings are bound to the lifetime
/// of the string pointed to by `str`.
/// @returns A pair of substrings.
std::pair<std::string_view, std::string_view>
split_once(std::string_view str, std::string_view sep);

/// Splits a character sequence into a vector of substrings, with escaping of
/// the separator.
/// @param str The string to split.
/// @param sep The separator where to split.
/// @param esc The escape string. If *esc* occurs immediately before
///            *sep*, then *sep* will not count as a separator.
///            In that case, *esc* will not be included in the output.
/// @param max_splits The maximum number of splits to perform.
/// @pre `!sep.empty() && !esc.empty()`
/// @returns A vector of substrings, with the separator escape strings removed.
std::vector<std::string>
split_escaped(std::string_view str, std::string_view sep, std::string_view esc,
              size_t max_splits = -1);

/// Constructs a `std::vector<std::string>` from a ::split result.
/// @param v The vector of iterator pairs from ::split.
/// @returns a vector of strings with the split elements.
std::vector<std::string> to_strings(const std::vector<std::string_view>& v);

/// Joins a sequence of strings according to a seperator.
/// @param begin The beginning of the sequence.
/// @param end The end of the sequence.
/// @param sep The string to insert between each element of the sequence.
/// @returns The joined string.
template <class Iterator, class Predicate>
std::string
join(Iterator begin, Iterator end, std::string_view sep, Predicate p) {
  std::string result;
  if (begin != end) {
    result += p(*begin++);
    for (; begin != end; ++begin) {
      result += sep;
      result += p(*begin);
    }
  }
  return result;
}

template <class Iterator>
std::string join(Iterator begin, Iterator end, std::string_view sep) {
  return join(begin, end, sep, [](auto&& x) -> decltype(x) {
    return x;
  });
}

template <class T>
std::string join(const std::vector<T>& v, std::string_view sep) {
  if constexpr (std::is_same_v<T, std::string>
                || std::is_same_v<T, std::string_view>) {
    return join(v.begin(), v.end(), sep);
  } else {
    auto pred = [](const T& x) {
      using std::to_string;
      return to_string(x);
    };
    return join(v.begin(), v.end(), sep, pred);
  }
}

} // namespace tenzir::detail
