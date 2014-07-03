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
    if (*i == '\\'
      && *(i + 1) == 'x'
      && std::isxdigit(*(i + 2))
      && std::isxdigit(*(i + 3)))
    {
      unesc += hex_to_byte(*(i + 2), *(i + 3));
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
  unesc.reserve(str.size());

  if (str.empty())
    return {};

  std::string::size_type i = 0;
  if (str[i] == '"')
    ++i;

  while (i < str.size() - 1)
  {
    auto c = str[i++];
    if (c == '"')
    {
      // Unescaped double-quotes not allowed inside a string.
      return {};
    }
    else if (c != '\\')
    {
      unesc += c;
      continue;
    }

    if (i == str.size() - 1)
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
      case 'u':
        {
          // We ignore unicode escape sequences.
          unesc += 'u';
          auto max = std::min(std::string::size_type{4}, str.size() - i - 1);
          for (std::string::size_type j = 0; j < max; ++j)
            unesc += str[i++];
        }
        break;
    }
  }

  if (i != str.size() - 1 || str[i] != '"')
    return {};

  return unesc;
}

} // namespace util
} // namespace vast
