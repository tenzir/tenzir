//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/string.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/escapers.hpp"

#include <cstring>
#include <string_view>
#include <vector>

namespace tenzir {
namespace detail {

auto quoting_escaping_policy::is_inside_of_quotes(
  std::string_view text, size_t idx) const noexcept -> bool {
  if (idx > text.size()) {
    return false;
  }
  auto start = size_t{0};
  while (start < text.size()) {
    auto open = find_opening_quote(text, start);
    if (open > idx) {
      return false;
    }
    auto close = find_closing_quote(text, open);
    if (close == text.npos) {
      return false;
    }
    if (close > idx) {
      return true;
    }
    start = close + 1;
  }
  return false;
}

auto quoting_escaping_policy::find_opening_quote(
  std::string_view text, size_t start) const -> std::string_view::size_type {
  auto active_escape = false;
  for (; start < text.size(); ++start) {
    const auto character = text[start];
    const auto is_quote = is_quote_character(character);
    const auto is_backslash = character == '\\';
    if (is_quote and not active_escape) {
      return start;
    } else if (is_backslash and not active_escape) {
      active_escape = backslashes_escape;
      continue;
    }
    active_escape = false;
  }
  return text.npos;
}

auto quoting_escaping_policy::find_closing_quote(
  std::string_view text, size_t opening) const -> std::string_view::size_type {
  auto active_escape = false;
  TENZIR_ASSERT(is_quote_character(text[opening]));
  for (size_t i = opening + 1; i < text.size(); ++i) {
    const auto character = text[i];
    const auto is_backslash = character == '\\';
    if (not active_escape and character == text[opening]) {
      if (doubled_quotes_escape and i < text.size() - 1
          and text[i + 1] == character) {
        ++i;
        continue;
      }
      return i;
    } else if (is_backslash and not active_escape) {
      active_escape = backslashes_escape;
      continue;
    }
    active_escape = false;
  }
  return text.npos;
}

auto quoting_escaping_policy::find_first_of_not_in_quotes(
  std::string_view text, std::string_view targets,
  size_t start) const -> std::string_view::size_type {
  auto quote_start = text.npos;
  auto active_escape = false;
  for (size_t i = start; i < text.size(); ++i) {
    const auto character = text[i];
    const auto maybe_closing = quote_start != text.npos
                               and character == text[quote_start]
                               and not active_escape;
    if (maybe_closing) {
      if (doubled_quotes_escape and i < text.size() - 1
          and text[i + 1] == character) {
        ++i;
      } else {
        quote_start = text.npos;
      }
      continue;
    }
    const bool is_quote = is_quote_character(character);
    if (is_quote) {
      quote_start = i;
      continue;
    }
    const auto is_target = targets.find(character) != text.npos;
    if (is_target) {
      if (quote_start == text.npos) {
        return i;
      }
      auto end_of_quote = find_closing_quote(text, quote_start);
      if (end_of_quote == text.npos) {
        return i;
      } else {
        i = end_of_quote;
        quote_start = text.npos;
      }
    }
  }
  return text.npos;
};

auto quoting_escaping_policy::find_first_not_in_quotes(
  std::string_view text, char target,
  size_t start) const -> std::string_view::size_type {
  return find_first_of_not_in_quotes(text, std::string_view{&target, 1}, start);
}

auto quoting_escaping_policy::is_quoted(std::string_view text) const noexcept
  -> bool {
  return text.size() >= 2 and text.front() == text.back()
         and is_quote_character(text.front())
         and find_closing_quote(text, 0) == text.size() - 1;
}

auto quoting_escaping_policy::unquote(std::string_view text) const
  -> std::string_view {
  if (is_quoted(text)) {
    text.remove_prefix(1);
    text.remove_suffix(1);
  }
  return text;
}

auto quoting_escaping_policy::unquote_unescape(std::string_view text) const
  -> std::string {
  if (text.size() < 2) {
    return std::string{text};
  }
  const auto orig_size = text.size();
  const auto quote_char = text.front();
  text = unquote(text);
  const bool is_quoted = text.size() < orig_size;
  auto result = std::string{};
  result.reserve(text.size());
  for (auto i = size_t{0}; i < text.size(); ++i) {
    if (backslashes_escape and text[i] == '\\' and i < text.size() - 1) {
      if (is_quote_character(text[i + 1])) {
        ++i;
      }
    } else if (doubled_quotes_escape and is_quoted and text[i] == quote_char
               and i < text.size() - 1) {
      if (text[i + 1] == quote_char) {
        ++i;
      }
    }
    result += text[i];
  }
  return result;
}

auto quoting_escaping_policy::split_at_unquoted(std::string_view text,
                                                char target) const
  -> std::pair<std::string_view, std::string_view> {
  const auto field_end = find_first_not_in_quotes(text, target, 0);
  auto first = text.substr(0, field_end);
  text.remove_prefix(std::min(first.size() + 1, text.size()));
  return {first, text};
}

std::string byte_escape(std::string_view str) {
  return escape(str, print_escaper);
}

std::string byte_escape(std::string_view str, const std::string& extra) {
  return escape(str, make_extra_print_escaper(extra));
}

std::string byte_escape_all(std::string_view str) {
  return escape(str, hex_escaper);
}

std::string byte_unescape(std::string_view str) {
  return unescape(str, byte_unescaper);
}

std::string control_char_escape(std::string_view str) {
  std::string result;
  result.reserve(str.size());
  auto f = str.begin();
  auto l = str.end();
  auto out = std::back_inserter(result);
  while (f != l)
    control_character_escaper(f, out);
  return result;
}

std::string json_escape(std::string_view str) {
  if (str.empty())
    return "\"\"";
  std::string result;
  result.reserve(str.size() + 2);
  result += '"';
  auto f = str.begin();
  auto l = str.end();
  auto out = std::back_inserter(result);
  while (f != l)
    json_escaper(f, out);
  result += '"';
  return result;
}

std::string json_unescape(std::string_view str) {
  // Unescape everything until the closing double quote.
  auto f = str.begin();
  auto l = str.end();
  // Need at least two delimiting double quotes.
  if (f == l || l - f < 2)
    return {};
  // Only consider double-quoted strings.
  if (!(*f++ == '"' && (*--l == '"')))
    return {};
  std::string result;
  result.reserve(str.size());
  auto out = std::back_inserter(result);
  while (f != l)
    if (!json_unescaper(f, l, out))
      return {};
  return result;
}

std::string percent_escape(std::string_view str) {
  return escape(str, percent_escaper);
}

std::string percent_unescape(std::string_view str) {
  return unescape(str, percent_unescaper);
}

std::string double_escape(std::string_view str, std::string_view esc) {
  return escape(str, make_double_escaper(esc));
}

std::string double_unescape(std::string_view str, std::string_view esc) {
  return unescape(str, make_double_unescaper(esc));
}

std::string replace_all(std::string str, std::string_view search,
                        std::string_view replace) {
  auto pos = std::string::size_type{0};
  while ((pos = str.find(search, pos)) != std::string::npos) {
    str.replace(pos, search.length(), replace);
    pos += replace.length();
  }
  return str;
}

std::vector<std::string_view>
split(std::string_view str, std::string_view sep, size_t max_splits) {
  TENZIR_ASSERT(!sep.empty());
  if (str.empty())
    return {""};
  std::vector<std::string_view> out;
  auto it = str.begin();
  size_t splits = 0;
  while (it != str.end() && splits++ != max_splits) {
    auto next_sep = std::ranges::search(std::string_view{it, str.end()}, sep);
    out.emplace_back(it, next_sep.begin());
    it = next_sep.end();
    // Final char in `str` is a separator ->
    // add empty element
    if (!next_sep.empty() && it == str.end())
      out.emplace_back("");
  }
  if (it != str.end())
    out.emplace_back(it, str.end());
  return out;
}

std::pair<std::string_view, std::string_view>
split_once(std::string_view str, std::string_view sep) {
  auto parts = split(str, sep, 1);
  TENZIR_ASSERT(parts.size() <= 2);
  if (parts.size() == 1) {
    return {parts[0], std::string_view{str.end(), str.end()}};
  } else {
    return {parts[0], parts[1]};
  }
}

std::vector<std::string>
split_escaped(std::string_view str, std::string_view sep, std::string_view esc,
              size_t max_splits) {
  TENZIR_ASSERT(!sep.empty());
  TENZIR_ASSERT(!esc.empty());
  if (str.empty())
    return {""};
  std::vector<std::string> out;
  auto it = str.begin();
  std::string current{};
  size_t splits = 0;
  while (it != str.end() && splits != max_splits) {
    auto next_sep = std::ranges::search(std::string_view{it, str.end()}, sep);
    if (std::distance(it, next_sep.begin()) >= std::ssize(esc)) {
      // Possibly escaped separator
      auto possible_esc = std::string_view{
        std::prev(next_sep.begin(), std::ssize(esc)), next_sep.begin()};
      if (possible_esc == esc) {
        current.append(std::string_view{it, possible_esc.begin()});
        current.append(std::string_view{next_sep.begin(), next_sep.end()});
        it = next_sep.end();
        continue;
      }
    }
    current.append(std::string_view{it, next_sep.begin()});
    out.emplace_back(std::move(current));
    current = {};
    it = next_sep.end();
    if (!next_sep.empty() && it == str.end())
      out.emplace_back("");
    ++splits;
  }
  if (it != str.end())
    current.append(std::string_view{it, str.end()});
  if (not current.empty())
    out.emplace_back(std::move(current));
  return out;
}

std::vector<std::string> to_strings(const std::vector<std::string_view>& v) {
  std::vector<std::string> strs;
  strs.resize(v.size());
  for (size_t i = 0; i < v.size(); ++i)
    strs[i] = v[i];
  return strs;
}

} // namespace detail
} // namespace tenzir
