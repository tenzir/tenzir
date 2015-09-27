#include <cstring>
#include <vector>

#include "vast/util/coding.h"
#include "vast/util/string.h"

namespace vast {
namespace util {

namespace {

template <class Iterator, class Escaper>
void escape(Iterator f, Iterator l, std::string& escaped, Escaper escaper) {
  escaped.reserve(l - f);
  auto out = std::back_inserter(escaped);
  while (f != l)
    escaper(f++, out);
}

template <class Iterator, class Unescaper>
bool unescape(Iterator f, Iterator l, std::string& unescaped,
              Unescaper unescaper) {
  unescaped.reserve(l - f);
  auto out = std::back_inserter(unescaped);
  while (f != l)
    if (unescaper(f, l, out))
      ++f;
    else
      return false;
  return true;
}

auto hex_escaper = [](auto in, auto out) {
  *out++ = '\\';
  *out++ = 'x';
  auto hex = byte_to_hex(*in);
  *out++ = hex.first;
  *out++ = hex.second;
};

auto hex_unescaper = [](auto& f, auto l, auto out) {
  VAST_ASSERT(l - f >= 2);
  auto hi = *++f;
  auto lo = *++f;
  if (! std::isxdigit(hi) || ! std::isxdigit(lo))
    return false;
  *out++ = hex_to_byte(hi, lo);
  return true;
};

auto print_escaper = [](auto in, auto out) {
  if (std::isprint(*in))
    *out++ = *in;
  else
    hex_escaper(in, out);
};

} // namespace <anonymous>

std::string byte_escape(std::string const& str) {
  std::string result;
  escape(str.begin(), str.end(), result, print_escaper);
  return result;
}

std::string byte_escape(std::string const& str, std::string const& extra) {
  auto extra_print_escaper = [&](auto in, auto out) {
    if (extra.find(*in) != std::string::npos) {
      *out++ = '\\';
      *out++ = *in;
    } else {
      print_escaper(in, out);
    }
  };
  std::string result;
  escape(str.begin(), str.end(), result, extra_print_escaper);
  return result;
}


std::string byte_escape_all(std::string const& str) {
  std::string result;
  escape(str.begin(), str.end(), result, hex_escaper);
  return result;
}

std::string byte_unescape(std::string const& str) {
  auto byte_unescaper = [](auto& f, auto l, auto out) {
    if (*f != '\\') {
      *out++ = *f;
      return true;
    }
    if (l - f < 4)
      return false; // Not enough input.
    if (*++f != 'x') {
      *out++ = *f++; // Remove escape backslashes that aren't \x.
      return true;
    }
    return hex_unescaper(f, l, out);
  };
  std::string result;
  if (! unescape(str.begin(), str.end(), result, byte_unescaper))
    return {};
  return result;
}

std::string json_escape(std::string const& str) {
  // The JSON RFC (http://www.ietf.org/rfc/rfc4627.txt) specifies the escaping
  // rules in section 2.5:
  //
  //    All Unicode characters may be placed within the quotation marks except
  //    for the characters that must be escaped: quotation mark, reverse
  //    solidus, and the control characters (U+0000 through U+001F).
  //
  //  That is, '"', '\\', and control characters are the only mandatory escaped
  //  values. The rest is optional.
  auto json_escaper = [](auto in, auto out) {
    auto escape_char = [](char c, auto out) {
      *out++ = '\\';
      *out++ = c;
    };
    switch (*in) {
      default:
        print_escaper(in, out);
        break;
      case '"':
      case '\\':
        escape_char(*in, out);
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
  };
  if (str.empty())
    return "\"\"";
  std::string result;
  result += '"';
  escape(str.begin(), str.end(), result, json_escaper);
  result += '"';
  return result;
}

std::string json_unescape(std::string const& str) {
  // Unescape everything until the closing double quote.
  auto json_unescaper = [](auto& f, auto l, auto out) {
    if (*f == '"') // Unescaped double-quotes not allowed.
      return false;
    if (*f != '\\') { // Skip every non-escape character.
      *out++ = *f;
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
        // We currently don't support unicode and leave \uXXXX as is.
        *out++ = '\\';
        *out++ = 'u';
        auto end = std::min(decltype(l - f){4}, l - f);
        for (auto i = 0; i < end; ++i)
          *out++ = *++f;
        break;
      }
      case 'x': {
        if (l - f < 3)
          return false; // Need \x##.
        auto hi = *++f;
        auto lo = *++f;
        if (! std::isxdigit(hi) || ! std::isxdigit(lo))
          return false;
        *out++ = hex_to_byte(hi, lo);
        break;
      }
    }
    return true;
  };
  auto f = str.begin();
  auto l = str.end();
  // Need at least two delimiting double quotes.
  if (f == l || l - f < 2)
    return {};
  // Only consider double-quoted strings.
  if (!(*f++ == '"' && (*--l == '"')))
    return {};
  std::string result;
  if (! unescape(f, l, result, json_unescaper))
    return {};
  return result;
}

std::string percent_escape(std::string const& str) {
  auto url_escaper = [](auto in, auto out) {
    auto is_unreserved = [](char c) {
      return std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~';
    };
    if (is_unreserved(*in)) {
      *out++ = *in;
    } else {
      *out++ = '%';
      auto hex = byte_to_hex(*in);
      *out++ = hex.first;
      *out++ = hex.second;
    }
  };
  std::string result;
  escape(str.begin(), str.end(), result, url_escaper);
  return result;
}

std::string percent_unescape(std::string const& str) {
  auto url_unescaper = [](auto& f, auto l, auto out) {
    if (*f != '%') {
      *out++ = *f;
      return true;
    }
    if (l - f < 3) // Need %xx
      return false;
    return hex_unescaper(f, l, out);
  };
  std::string result;
  if (! unescape(str.begin(), str.end(), result, url_unescaper))
    return {};
  return result;
}

} // namespace util
} // namespace vast
