/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include <cstring>
#include <vector>

#include "vast/detail/assert.hpp"
#include "vast/detail/escapers.hpp"
#include "vast/detail/string.hpp"

namespace vast {
namespace detail {

std::string byte_escape(const std::string& str) {
  return escape(str, print_escaper);
}

std::string byte_escape(const std::string& str, const std::string& extra) {
  auto print_extra_escaper = [&](auto& f, auto l, auto out) {
    if (extra.find(*f) != std::string::npos) {
      *out++ = '\\';
      *out++ = *f++;
    } else {
      print_escaper(f, l, out);
    }
  };
  return escape(str, print_extra_escaper);
}

std::string byte_escape_all(const std::string& str) {
  return escape(str, hex_escaper);
}

std::string byte_unescape(const std::string& str) {
  return unescape(str, byte_unescaper);
}

std::string json_escape(const std::string& str) {
  if (str.empty())
    return "\"\"";
  std::string result;
  result.reserve(str.size() + 2);
  result += '"';
  auto f = str.begin();
  auto l = str.end();
  auto out = std::back_inserter(result);
  while (f != l)
    json_escaper(f, l, out);
  result += '"';
  return result;
}

std::string json_unescape(const std::string& str) {
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

std::string percent_escape(const std::string& str) {
  return escape(str, percent_escaper);
}

std::string percent_unescape(const std::string& str) {
  return unescape(str, percent_unescaper);
}

std::string double_escape(const std::string& str, const std::string& esc) {
  return escape(str, double_escaper(esc));
}

std::string double_unescape(const std::string& str, const std::string& esc) {
  return unescape(str, double_unescaper(esc));
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

std::vector<std::string_view> split(std::string_view str, std::string_view sep,
                                    std::string_view esc, size_t max_splits,
                                    bool include_sep) {
  VAST_ASSERT(!sep.empty());
  std::vector<std::string_view> pos;
  size_t splits = 0;
  auto end = str.end();
  auto begin = str.begin();
  auto i = begin;
  auto prev = i;
  auto push = [&](auto first, auto last) {
    using std::distance;
    pos.emplace_back(str.substr(distance(begin, first), distance(first, last)));
  };
  while (i != end) {
    // Find a separator that fits in the string.
    if (*i != sep[0] || i + sep.size() > end) {
      ++i;
      continue;
    }
    // Check remaining separator characters.
    size_t j = 1;
    auto s = i;
    while (j < sep.size())
      if (*++s != sep[j])
        break;
      else
        ++j;
    // No separator match.
    if (j != sep.size()) {
      ++i;
      continue;
    }
    // Make sure it's not an escaped match.
    if (!esc.empty() && esc.size() < static_cast<size_t>(i - begin)) {
      auto escaped = true;
      auto esc_start = i - esc.size();
      for (size_t j = 0; j < esc.size(); ++j)
        if (esc_start[j] != esc[j]) {
          escaped = false;
          break;
        }
      if (escaped) {
        ++i;
        continue;
      }
    }
    if (splits++ == max_splits)
      break;
    push(prev, i);
    if (include_sep)
      push(i, i + sep.size());
    i += sep.size();
    prev = i;
  }
  if (prev != end)
      push(prev, end);
  return pos;
}

std::vector<std::string> to_strings(const std::vector<std::string_view>& v) {
  std::vector<std::string> strs;
  strs.resize(v.size());
  for (size_t i = 0; i < v.size(); ++i)
    strs[i] = v[i];
  return strs;
}

} // namespace detail
} // namespace vast
