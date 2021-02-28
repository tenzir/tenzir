//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/coding.hpp"

#include <fmt/format.h>

#include <array>
#include <cctype>
#include <string>

namespace vast::detail {

inline auto hex_escaper = [](auto& f, auto out) {
  auto hex = byte_to_hex(*f++);
  *out++ = '\\';
  *out++ = 'x';
  *out++ = hex.first;
  *out++ = hex.second;
};

inline auto hex_unescaper = [](auto& f, auto l, auto out) {
  auto hi = *f++;
  if (f == l)
    return false;
  auto lo = *f++;
  if (!std::isxdigit(hi) || !std::isxdigit(lo))
    return false;
  *out++ = hex_to_byte(hi, lo);
  return true;
};

inline auto print_escaper = [](auto& f, auto out) {
  if (std::isprint(*f))
    *out++ = *f++;
  else
    hex_escaper(f, out);
};

inline auto byte_unescaper = [](auto& f, auto l, auto out) {
  if (*f != '\\') {
    *out++ = *f++;
    return true;
  }
  if (l - f < 4)
    return false; // Not enough input.
  if (*++f != 'x') {
    *out++ = *f++; // Remove escape backslashes that aren't \x.
    return true;
  }
  return hex_unescaper(++f, l, out);
};

// The JSON RFC (http://www.ietf.org/rfc/rfc4627.txt) specifies the escaping
// rules in section 2.5:
//
//    All Unicode characters may be placed within the quotation marks except
//    for the characters that must be escaped: quotation mark, reverse
//    solidus, and the control characters (U+0000 through U+001F).
//
// That is, '"', '\\', and control characters are the only mandatory escaped
// values. The rest is optional.
inline auto json_escaper = [](auto& f, auto out) {
  auto escape_char = [](char c, auto out) {
    *out++ = '\\';
    *out++ = c;
  };
  auto json_print_escaper = [](auto& f, auto out) {
    if (!std::iscntrl(*f)) {
      *out++ = *f++;
    } else {
      auto hex = byte_to_hex(*f++);
      *out++ = '\\';
      *out++ = 'u';
      *out++ = '0';
      *out++ = '0';
      *out++ = hex.first;
      *out++ = hex.second;
    }
  };
  switch (*f) {
    default:
      json_print_escaper(f, out);
      return;
    case '"':
    case '\\':
      escape_char(*f, out);
      break;
    case '\b':
      escape_char('b', out);
      break;
    case '\f':
      escape_char('f', out);
      break;
    case '\r':
      escape_char('r', out);
      break;
    case '\n':
      escape_char('n', out);
      break;
    case '\t':
      escape_char('t', out);
      break;
  }
  ++f;
};

inline auto json_unescaper = [](auto& f, auto l, auto out) {
  if (*f == '"') // Unescaped double-quotes not allowed.
    return false;
  if (*f != '\\') { // Skip every non-escape character.
    *out++ = *f++;
    return true;
  }
  if (l - f < 2)
    return false; // Need at least one char after \.
  switch (auto c = *++f) {
    default:
      return false;
    case '\\':
      *out++ = '\\';
      break;
    case '"':
      *out++ = '"';
      break;
    case '/':
      *out++ = '/';
      break;
    case 'b':
      *out++ = '\b';
      break;
    case 'f':
      *out++ = '\f';
      break;
    case 'r':
      *out++ = '\r';
      break;
    case 'n':
      *out++ = '\n';
      break;
    case 't':
      *out++ = '\t';
      break;
    case 'u': {
      // We currently only support single-byte escapings and any unicode escape
      // sequence other than \u00XX as is.
      if (l - f < 4)
        return false;
      std::array<char, 4> bytes{{0, 0, 0, 0}};
      bytes[0] = *++f;
      bytes[1] = *++f;
      bytes[2] = *++f;
      bytes[3] = *++f;
      if (bytes[0] != '0' || bytes[1] != '0') {
        // Leave input as is, we don't know how to handle it (yet).
        *out++ = '\\';
        *out++ = 'u';
        std::copy(bytes.begin(), bytes.end(), out);
      } else {
        // Hex-unescape the XX portion of \u00XX.
        if (!std::isxdigit(bytes[2]) || !std::isxdigit(bytes[3]))
          return false;
        *out++ = hex_to_byte(bytes[2], bytes[3]);
      }
      break;
    }
  }
  ++f;
  return true;
};

inline auto percent_escaper = [](auto& f, auto out) {
  auto is_unreserved = [](char c) {
    return std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~';
  };
  if (is_unreserved(*f)) {
    *out++ = *f++;
  } else {
    auto hex = byte_to_hex(*f++);
    *out++ = '%';
    *out++ = hex.first;
    *out++ = hex.second;
  }
};

inline auto percent_unescaper = [](auto& f, auto l, auto out) {
  if (*f != '%') {
    *out++ = *f++;
    return true;
  }
  if (l - f < 3) // Need %xx
    return false;
  return hex_unescaper(++f, l, out);
};

inline auto make_extra_print_escaper(std::string_view extra) {
  return [=](auto& f, auto out) {
    if (extra.find(*f) != std::string_view::npos) {
      *out++ = '\\';
      *out++ = *f++;
    } else {
      print_escaper(f, out);
    }
  };
}

inline auto make_double_escaper(std::string_view esc) {
  return [=](auto& f, auto out) {
    if (esc.find(*f) != std::string_view::npos)
      *out++ = *f;
    *out++ = *f++;
  };
}

inline auto make_double_unescaper(std::string_view esc) {
  return [=](auto& f, auto l, auto out) -> bool {
    auto x = *f++;
    if (f == l) {
      *out++ = x;
      return true;
    }
    *out++ = x;
    auto y = *f++;
    if (x == y && esc.find(x) == std::string_view::npos)
      *out++ = y;
    return true;
  };
}

} // namespace vast::detail
