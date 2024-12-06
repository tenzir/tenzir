//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/assert.hpp"

#include <algorithm>
#include <iterator>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace tenzir::detail {

constexpr inline std::string_view ascii_whitespace = " \t\r\n\f\v";
/// trims leading whitespace of string according to the given whitespace
/// @param value the string to trim
/// @param whitespace a string of characters, each of white is considered
/// whitespace
/// @returns a string_view without leading whitespace
inline auto
trim_front(std::string_view value, const std::string_view whitespace
                                   = ascii_whitespace) -> std::string_view {
  if (value.empty()) {
    return value;
  }
  const auto first_character = value.find_first_not_of(whitespace);
  if (first_character != value.npos) {
    value.remove_prefix(first_character);
  }
  return value;
}

/// trims trailing whitespace of string according to the given whitespace
/// @param value the string to trim
/// @param whitespace a string of characters, each of white is considered
/// whitespace
/// @returns a string_view without trailing whitespace
inline auto
trim_back(std::string_view value, const std::string_view whitespace
                                  = ascii_whitespace) -> std::string_view {
  if (value.empty()) {
    return value;
  }
  const auto last_character = value.find_last_not_of(whitespace);
  if (last_character != value.npos) {
    value.remove_suffix(value.size() - last_character - 1);
  }
  return value;
}

/// trims a string according to the given whitespace
/// @param value the string to trim
/// @param whitespace a string of characters, each of white is considered
/// whitespace
/// @returns a string_view without leading or trailing whitespace
inline auto
trim(std::string_view value, const std::string_view whitespace
                             = ascii_whitespace) -> std::string_view {
  value = trim_front(value, whitespace);
  value = trim_back(value, whitespace);
  return value;
}

inline auto is_escaped_at(std::string_view text, size_t i) -> bool {
  auto slashes = size_t{0};
  while (i > 0) {
    --i;
    if (text[i] == '\\') {
      ++slashes;
    } else {
      break;
    }
  }
  return slashes % 2 == 1;
}

struct quoting_escaping_policy {
  std::string quotes = "\"\'";
  bool backslashes_escape = true;
  bool doubled_quotes_escape = false;

  /// Checks whether position `idx` in the whole string `text` is enclosed in
  /// quotes. That is: There is an opening quote before `idx` and a closing
  /// quote after `idx`.
  /// Finds the next *opening* quote
  /// @param text the text to consider
  /// @param idx the index to check
  auto
  is_inside_of_quotes(std::string_view text, size_t idx) const noexcept -> bool;

  /// Returns whether character `c` is a quote character.
  auto is_quote_character(char c) const -> bool {
    return quotes.find(c) != quotes.npos;
  }

  /// Finds the next *opening* quote
  /// @param text the text to search
  /// @param start the index to start at
  /// @returns the index of the next opening quote character that is not
  /// escaped; `npos` otherwise
  auto
  find_opening_quote(std::string_view text,
                     size_t start = 0) const -> std::string_view::size_type;

  /// Finds the closing quote matching the opening at `opening`
  /// @param text The text to search
  /// @param opening The index where the quote starts
  /// @pre `is_quote_character(text[opening])` holds
  /// @returns the index where the quote is closed `npos` otherwise
  auto find_closing_quote(std::string_view text,
                          size_t opening) const -> std::string_view::size_type;

  /// finds the index of the first occurrence that is not enclosed my matching
  /// quotes quotes that are not closed are not considered quoting anything
  /// @param s the string to search
  /// @param targets a list of characters to search for
  /// @param quotes list of characters to consider as "quotes"
  /// @param start index to start the search at
  /// @pre there must not be any intersection between `quotes` and `targets`
  /// @returns index of the first occurrence of a character from `find` that
  /// is`not enclosed by matching `quotes`; `npos` otherwise
  auto
  find_first_of_not_in_quotes(std::string_view text, std::string_view targets,
                              size_t start
                              = 0) const -> std::string_view::size_type;

  /// finds the index of the first occurrence of a character that is not
  /// enclosed my matching quotes quotes that are not closed are not considered
  /// quoting anything
  /// @param s the string to search
  /// @param target a character to search for
  /// @param start index to start the serach at
  /// @param quotes list of characters to consider as "quotes"
  /// @pre there must not be any intersection between `quotes` and `target`
  /// @returns index of the first occurrence of character `find` that is`not
  /// enclosed by matching `quotes`; `npos` otherwise
  auto find_first_not_in_quotes(std::string_view text, char target,
                                size_t start
                                = 0) const -> std::string_view::size_type;

  /// Check whether `text` is quoted, that is it starts with a quote character
  /// and the quote is by the final character
  auto is_quoted(std::string_view text) const noexcept -> bool;

  /// Unquotes a string, if it is enclosed in matching quotes that are not escaped
  auto unquote(std::string_view text) const -> std::string_view;

  /// Unescapes all escaped quote in the string
  auto unescape_quotes(std::string_view text) const -> std::string;

  /// Unquotes a string and unescapes all quotes inside of it.
  /// Equivalent to `unquote(unescape_quotes(text))`
  auto unquote_unescape(std::string_view text) const -> std::string;

  /// Splits a string at the first `target`, that is not enclosed in quote
  auto split_at_unquoted(std::string_view text, char target) const
    -> std::pair<std::string_view, std::string_view>;

  friend auto inspect(auto& f, quoting_escaping_policy& x) -> bool {
    return f.object(x)
      .pretty_name("quoting_escaping_policy")
      .fields(f.field("quotes", x.quotes),
              f.field("backslashes_escape", x.backslashes_escape),
              f.field("doubled_quotes_escape", x.doubled_quotes_escape));
  }
};

/// Unescapes a string according to an escaper.
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
  while (f != l) {
    escaper(f, out);
  }
  return result;
}

/// Unescapes a string according to an unescaper.
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
  while (f != l) {
    if (!unescaper(f, l, out)) {
      return {};
    }
  }
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
