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

std::string replace_all(std::string str, const std::string& search,
                        const std::string& replace) {
  auto pos = std::string::size_type{0};
  while ((pos = str.find(search, pos)) != std::string::npos) {
     str.replace(pos, search.length(), replace);
     pos += replace.length();
  }
  return str;
}

} // namespace detail
} // namespace vast
