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
#include <vector>

namespace tenzir {
namespace detail {

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
