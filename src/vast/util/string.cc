#include <vector>

#include "vast/util/coding.h"
#include "vast/util/string.h"

namespace vast {
namespace util {

namespace {

static constexpr char hex[] = "0123456789abcdef";

} // namespace <anonymous>

std::string byte_escape(std::string const& str)
{
  std::string esc;
  esc.reserve(str.size());
  for (auto c : str)
    if (std::isprint(c))
    {
      esc += c;
    }
    else
    {
      esc += '\\';
      esc += 'x';
      esc += hex[(c & 0xf0) >> 4];
      esc += hex[c & 0x0f];
    }
  return esc;
}

std::string byte_escape(std::string const& str, std::string const& extra)
{
  std::string esc;
  esc.reserve(str.size());
  for (auto c : str)
    if (std::isprint(c))
    {
      if (extra.find(c) != std::string::npos)
        esc += '\\';
      esc += c;
    }
    else
    {
      esc += '\\';
      esc += 'x';
      esc += hex[(c & 0xf0) >> 4];
      esc += hex[c & 0x0f];
    }
  return esc;
}

std::string byte_escape_all(std::string const& str)
{
  std::string esc;
  esc.resize(str.size() * 4);
  auto i = std::string::size_type{0};
  for (auto c : str)
  {
    esc[i++] = '\\';
    esc[i++] = 'x';
    esc[i++] = hex[(c & 0xf0) >> 4];
    esc[i++] = hex[c & 0x0f];
  }
  return esc;
}

std::string byte_unescape(std::string const& str)
{
  std::string unesc;
  auto i = str.begin();
  auto last = str.end();
  while (i != last)
  {
    auto c = *i++;
    if (c != '\\')
    {
      unesc += c;
    }
    else if (i == last)
    {
      return {}; // malformed string with dangling '\' at the end.
    }
    else
    {
      switch ((c = *i++))
      {
        default:
          unesc += c;
          break;
        case 'x':
          if (i != last && i + 1 != last
              && std::isxdigit(i[0]) && std::isxdigit(i[1]))
          {
            auto hi = *i++;
            auto lo = *i++;
            unesc += hex_to_byte(hi, lo);
          }
          else
          {
            unesc += 'x';
          }
          break;
      }
    }
  }
  return unesc;
}

std::string json_escape(std::string const& str)
{
  if (str.empty())
    return "\"\"";
  std::string esc;
  esc.reserve(str.size() + 2);
  esc += '"';
  // The JSON RFC (http://www.ietf.org/rfc/rfc4627.txt) specifies the escaping
  // rules in section 2.5:
  //
  //    All Unicode characters may be placed within the quotation marks except
  //    for the characters that must be escaped: quotation mark, reverse
  //    solidus, and the control characters (U+0000 through U+001F).
  //
  //  That is, '"', '\\', and control characters are the only mandatory escaped
  //  values. The rest is optional.
  for (auto c : str)
  {
    switch (c)
    {
      default:
        if (std::isprint(c))
        {
          esc += c;
        }
        else
        {
          esc += '\\';
          esc += 'x';
          esc += hex[(c & 0xf0) >> 4];
          esc += hex[c & 0x0f];
        }
        break;
      case '"':
        esc += "\\\"";
        break;
      case '\\':
        esc += "\\\\";
        break;
      case '\b':
        esc += "\\b";
        break;
      case '\f':
        esc += "\\f";
        break;
      case '\r':
        esc += "\\r";
        break;
      case '\n':
        esc += "\\n";
        break;
      case '\t':
        esc += "\\t";
        break;
    }
  }
  esc += '"';
  return esc;
}

std::string json_unescape(std::string const& str)
{
  std::string unesc;
  if (str.empty() || str.size() < 2)
    return {};
  // Only consider double-quote strings.
  if (! (str.front() == '"' && str.back() == '"'))
    return {};
  unesc.reserve(str.size());
  std::string::size_type i = 1;
  std::string::size_type last = str.size() - 1;
  // Skip the opening double quote.
  // Unescape everything until the closing double quote.
  while (i < last)
  {
    auto c = str[i++];
    if (c == '"')   // Unescaped double-quotes not allowed.
      return {};
    if (c != '\\')  // Skip everything non-escpaed character.
    {
      unesc += c;
      continue;
    }
    if (i == last)  // No '\' before final double quote allowed.
      return {};
    switch (str[i++])
    {
      default:
        return {};
      case '\\':
        unesc += '\\';
        break;
      case '"':
        unesc += '"';
        break;
      case '/':
        unesc += '/';
        break;
      case 'b':
        unesc += '\b';
        break;
      case 'f':
        unesc += '\f';
        break;
      case 'r':
        unesc += '\r';
        break;
      case 'n':
        unesc += '\n';
        break;
      case 't':
        unesc += '\t';
        break;
      case 'u':    // We can't handle unicode and leave \uXXXX as is.
        {
          unesc += '\\';
          unesc += 'u';
          auto end = std::min(std::string::size_type{4}, last - i);
          for (std::string::size_type j = 0; j < end; ++j)
            unesc += str[i++];
        }
        break;
      case 'x':
        if (i + 1 < last)
        {
          auto hi = str[i++];
          auto lo = str[i++];
          if (std::isxdigit(hi) && std::isxdigit(lo))
          {
            unesc += hex_to_byte(hi, lo);
            break;
          }
        }
        return {}; // \x must be followed by two hex bytes.
    }
  }
  VAST_ASSERT(i == last);
  return unesc;
}

} // namespace util
} // namespace vast
