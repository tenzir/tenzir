#include "vast/util/string.h"

#include <vector>

#include "vast/util/coding.h"

namespace vast {
namespace util {

namespace {

static constexpr char hex[] = "0123456789abcdef";

} // namespace <anonymous>

std::string byte_escape(std::string const& str, bool all)
{
  if (str.empty())
    return {};
  std::string esc;
  if (all)
  {
    esc.resize(str.size() * 4);
    std::string::size_type i = 0;
    for (auto c : str)
    {
      esc[i++] = '\\';
      esc[i++] = 'x';
      esc[i++] = hex[(c & 0xf0) >> 4];
      esc[i++] = hex[c & 0x0f];
    }
  }
  else
  {
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
  }
  return esc;
}

std::string byte_unescape(std::string const& str)
{
  std::string unesc;
  auto i = str.begin();
  while (str.end() - i > 3)
  {
    if (*i == '\\' && i[1] == 'x' && std::isxdigit(i[2]) && std::isxdigit(i[3]))
    {
      unesc += hex_to_byte(i[2], i[3]);
      i += 4;
    }
    else
    {
      unesc += *i++;
    }
  }
  std::copy(i, str.end(), std::back_inserter(unesc));
  return unesc;
}

std::string json_escape(std::string const& str)
{
  if (str.empty())
    return "\"\"";
  std::string esc;
  esc.reserve(str.size());
  esc += '"';
  for (auto c : str)
  {
    switch (c)
    {
      default:
        esc += c;
        break;
      case '"':
        esc += "\\\"";
        break;
      case '\\':
        esc += "\\\\";
        break;
      case '/':
        esc += "\\/";
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
  // Only consider doulbe-quote strings.
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
    }
  }
  VAST_ASSERT(i == last);
  return unesc;
}

} // namespace util
} // namespace vast
