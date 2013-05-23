#ifndef VAST_PARSE_H
#define VAST_PARSE_H

#include <cassert>
#include <cstring>
#include "vast/container.h"
#include "vast/value.h"
#include "vast/detail/parse.h"

namespace vast {

// Forward declaration
template <typename Iterator>
bool parse(Iterator& start, Iterator end, value& x, value_type type);

template <typename Iterator>
bool parse(Iterator& start, Iterator /* end */, bool& x)
{
  switch (*start)
  {
    default:
      return false;
    case 'T':
      {
        ++start;
        x = true;
      }
      return true;
    case 'F':
      {
        ++start;
        x = false;
      }
      return true;
  }
}

template <typename Iterator>
bool parse(Iterator& start, Iterator end, int64_t& x)
{
  switch (*start)
  {
    default:
      return false;
    case '-':
        return detail::parse_negative_decimal(++start, end, x);
    case '+':
      ++start;
    case '0':
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
    case '8':
    case '9':
      return detail::parse_positive_decimal(start, end, x);
  }
}

template <typename Iterator>
bool parse(Iterator& start, Iterator end, uint64_t& x)
{
  return detail::parse_positive_decimal(start, end, x);
}

template <typename Iterator>
bool parse(Iterator& start, Iterator end, double& x)
{
  auto before = start;

  // Longest double: ~53 bytes.
  char buf[64];
  auto p = buf;
  while (start != end && p < &buf[63])
    *p++ = *start++;
  *p = '\0';

  x = detail::to_double(buf);

  // The only way to test whether parsing succeeded is unfortunately to check
  // whether we consumed any characters.
  return before != start;
}

template <typename Iterator>
bool parse(Iterator& start, Iterator end, time_range& x)
{
  char buf[32];
  auto p = buf;
  auto is_double = false;
  while (start != end && p < &buf[31])
  {
    if (*start == '.')
      is_double = true;
    else if (! std::isdigit(*start))
      break;

    *p++ = *start++;
  }

  *p = '\0';

  bool success = false;
  if (is_double)
  {
    double d = detail::to_double(buf);
    x = time_range::fractional(d);
    return true;
  }

  time_range::rep i;
  auto s = buf;
  success = detail::parse_positive_decimal(s, p, i);
  if (! success)
    return false;

  // Account for suffix.
  if (start == end)
    x = time_range::seconds(i);
  else
    switch (*start++)
    {
      default:
        return false;
      case 'n':
        if (start != end && *start++ == 's')
          x = time_range::nanoseconds(i);
        break;
      case 'u':
        if (start != end && *start++ == 's')
          x = time_range::microseconds(i);
        break;
      case 'm':
        if (start != end && *start++ == 's')
          x = time_range::milliseconds(i);
        else
          x = time_range::minutes(i);
        break;
      case 's':
        x = time_range::seconds(i);
        break;
      case 'h':
        x = time_range::hours(i);
        break;
    }

  return true;
}

template <typename Iterator>
bool parse(Iterator& start, Iterator end, time_point& x)
{
  time_range range;
  if (! parse(start, end, range))
    return false;

  x = time_point(range);

  return true;
}

template <typename Iterator>
bool parse(Iterator& start, Iterator end, time_point& x, char const* fmt)
{
  x = time_point({start, end}, fmt);
  start += end - start;

  return true;
}

template <typename Iterator>
bool parse(Iterator& start, Iterator end, string& x)
{
  string s(start, end);
  start += end - start;
  x = s.unescape();

  return true;
}

template <typename Iterator>
bool parse(Iterator& start, Iterator end, regex& x)
{
  if (*start != '/')
    return false;

  string s;
  auto success = parse(start, end, s);
  if (! success)
    return false;

  if (s.empty() || s[s.size() - 1] != '/')
    return false;

  x = regex(s.thin("/", "\\"));
  return true;
}

template <typename Iterator>
bool parse(Iterator&, Iterator, vector&)
{
  assert(! "not yet implemented");
  return false;
}

template <typename Iterator>
bool parse(Iterator& start,
           Iterator end,
           set& x,
           value_type elem_type,
           string const& sep = ", ",
           string const& esc = "\\")
{
  if (start == end)
    return false;

  string str;
  auto success = parse(start, end, str);
  if (! success || str.empty())
    return false;

  auto l = str.starts_with("{");
  auto r = str.ends_with("}");
  if (l && r)
    str = str.trim("{", "}");
  else if (l || r)
    return false;

  x.clear();
  value v;
  for (auto p : str.split(sep, esc))
    if (parse(p.first, p.second, v, elem_type))
      x.insert(std::move(v));

  return true;
}

template <typename Iterator>
bool parse(Iterator&, Iterator, table&)
{
  assert(! "not yet implemented");
  return false;
}

template <typename Iterator>
bool parse(Iterator&, Iterator, record&)
{
  assert(! "not yet implemented");
  return false;
}

template <typename Iterator>
bool parse(Iterator& start, Iterator end, address& x)
{
  string str;
  auto success = parse(start, end, str);
  if (! success)
    return false;

  try
  {
    x = address(str);
  }
  catch (error::bad_value const& e)
  {
    return false;
  }

  return true;
}

template <typename Iterator>
bool parse(Iterator& start, Iterator end, prefix& x)
{
  char buf[64];
  auto p = buf;
  while (*start != '/' && start != end && p < &buf[63])
    *p++ = *start++;
  *p = '\0';

  try
  {
    address addr(buf);

    if (*start++ != '/')
      return false;

    p = buf;
    while (start != end && p < &buf[3])
      *p++ = *start++;
    *p = '\0';

    uint8_t length;
    auto s = buf;
    auto success = detail::parse_positive_decimal(s, p, length);
    if (! success)
      return false;

    x = prefix(std::move(addr), length);
  }
  catch (error::bad_value const& e)
  {
    return false;
  }

  return true;
}

template <typename Iterator>
bool parse(Iterator& start, Iterator end, port& x)
{
  // Longest port: 42000/unknown = 5 + 1 + 7 = 13 bytes plus NUL.
  char buf[16];
  auto p = buf;
  while (*start != '/' && std::isdigit(*start) && start != end && p < &buf[15])
    *p++ = *start++;

  uint16_t number;
  auto s = buf;
  auto success = detail::parse_positive_decimal(s, p, number);
  if (! success)
    return false;

  if (start == end || *start++ != '/')
  {
    x = port(number, port::unknown);
    return true;
  }

  p = buf;
  while (start != end && p < &buf[7])
    *p++ = *start++;
  *p = '\0';

  if (! std::strncmp(buf, "tcp", 3))
    x = port(number, port::tcp);
  else if (! std::strncmp(buf, "udp", 3))
    x = port(number, port::udp);
  else if (! std::strncmp(buf, "icmp", 4))
    x = port(number, port::icmp);
  else
    x = port(number, port::unknown);

  return true;
}

/// Parses an arbitrary value. This function is powerful, yet slow: it can
/// parse *any* value type based on the value grammar.
///
/// @param str The string representation of the value.
///
/// @param x The parsed value.
///
/// @return `true` iff a value could be parsed and the full input be consumed.
bool parse(std::string const& str, value& v);

template <typename Iterator>
bool parse(Iterator& start, Iterator end, value& x, value_type type)
{
#define VAST_PARSE_CASE(type)                      \
      {                                          \
        type t;                                  \
        auto success = parse(start, end, t);     \
        if (success)                             \
          x = t;                                 \
                                                 \
        return success;                          \
      }

  switch (type)
  {
    default:
      throw error::parse("no known parser for this type");
    case bool_type:
      VAST_PARSE_CASE(bool)
    case int_type:
      VAST_PARSE_CASE(int64_t)
    case uint_type:
      VAST_PARSE_CASE(uint64_t)
    case double_type:
      VAST_PARSE_CASE(double)
    case time_range_type:
      VAST_PARSE_CASE(time_range)
    case time_point_type:
      VAST_PARSE_CASE(time_point)
    case string_type:
      VAST_PARSE_CASE(string)
    case regex_type:
      VAST_PARSE_CASE(regex)
    case address_type:
      VAST_PARSE_CASE(address)
    case prefix_type:
      VAST_PARSE_CASE(prefix)
    case port_type:
      VAST_PARSE_CASE(port)
  }
}

} // namespace vast

#endif
