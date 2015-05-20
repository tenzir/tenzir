#ifndef VAST_UTIL_STRING_H
#define VAST_UTIL_STRING_H

#include <cassert>
#include <algorithm>
#include <string>
#include <vector>

namespace vast {
namespace util {

/// Escapes all non-printable characters in a string with `\xAA` where `AA` is
/// the byte in hexadecimal representation.
/// @param str The string to escape.
/// @param all If `true` escapes every single character in *str*.
/// @returns The escaped string of *str*.
/// @relates byte_unescape
std::string byte_escape(std::string const& str, bool all = false);

/// Unescapes a byte-escaped string, i.e., replaces all occurrences of `\xAA`
/// with the value of the byte `AA`.
/// @param str The string to unescape.
/// @returns The unescaped string of *str*.
/// @relates byte_escape
std::string byte_unescape(std::string const& str);

/// Escapes a string according to JSON escaping.
/// @param str The string to escape.
/// @returns The escaped string.
/// @relates json_unescape
std::string json_escape(std::string const& str);

/// Unescapes a string escaped with JSON escaping.
/// @param str The string to unescape.
/// @returns The unescaped string.
/// @relates json_escape
std::string json_unescape(std::string const& str);

/// Splits a string into a vector of iterator pairs representing the
/// *[start, end)* range of each element.
/// @tparam Iterator A random-access iterator to a character sequence.
/// @param begin The beginning of the string to split.
/// @param end The end of the string to split.
/// @param sep The seperator where to split.
/// @param esc The escape string. If *esc* occurrs immediately in front of
///            *sep*, then *sep* will not count as a separator.
/// @param max_splits The maximum number of splits to perform.
/// @param include_sep If `true`, also include the separator after each
///                    match.
/// @pre `! sep.empty()`
/// @returns A vector of iterator pairs each of which delimit a single field
///          with a range *[start, end)*.
template <typename Iterator>
std::vector<std::pair<Iterator, Iterator>>
split(Iterator begin, Iterator end, std::string const& sep,
      std::string const& esc = "", size_t max_splits = -1,
      bool include_sep = false)
{
  assert(! sep.empty());
  std::vector<std::pair<Iterator, Iterator>> pos;
  size_t splits = 0;
  auto i = begin;
  auto prev = i;
  while (i != end)
  {
    // Find a separator that fits in the string.
    if (*i != sep[0] || i + sep.size() > end)
    {
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
    if (j != sep.size())
    {
      ++i;
      continue;
    }
    // Make sure it's not an escaped match.
    if (! esc.empty() && esc.size() < static_cast<size_t>(i - begin))
    {
      auto escaped = true;
      auto esc_start = i - esc.size();
      for (size_t j = 0; j < esc.size(); ++j)
        if (esc_start[j] != esc[j])
        {
          escaped = false;
          break;
        }
      if (escaped)
      {
        ++i;
        continue;
      }
    }
    if (splits++ == max_splits)
      break;
    pos.emplace_back(prev, i);
    if (include_sep)
      pos.emplace_back(i, i + sep.size());
    i += sep.size();
    prev = i;
  }
  if (prev != end)
    pos.emplace_back(prev, end);
  return pos;
}

std::vector<std::pair<std::string::const_iterator, std::string::const_iterator>>
inline split(std::string const& str, std::string const& sep,
             std::string const& esc = "", size_t max_splits = -1,
             bool include_sep = false)
{
  return split(str.begin(), str.end(), sep, esc, max_splits, include_sep);
}

/// Constructs a `std::vector<std::string>` from a ::split result.
/// @param v The vector of iterator pairs from ::split.
/// @returns a vector of strings with the split elements.
template <typename Iterator>
auto to_strings(std::vector<std::pair<Iterator, Iterator>> const& v)
{
  std::vector<std::string> strs;
  strs.resize(v.size());
  for (size_t i = 0; i < v.size(); ++i)
    strs[i] = {v[i].first, v[i].second};
  return strs;
}

/// Combines ::split and ::to_strings.
template <typename Iterator>
auto split_to_str(Iterator begin, Iterator end, std::string const& sep,
                  std::string const& esc = "", size_t max_splits = -1,
                  bool include_sep = false)
{
  return to_strings(split(begin, end, sep, esc, max_splits, include_sep));
}

inline auto split_to_str(std::string const& str, std::string const& sep,
                         std::string const& esc = "", size_t max_splits = -1,
                         bool include_sep = false)
{
  return split_to_str(str.begin(), str.end(), sep, esc, max_splits,
                      include_sep);
}

/// Joins a sequence of strings according to a seperator.
/// @param begin The beginning of the sequence.
/// @param end The end of the sequence.
/// @param sep The string to insert between each element of the sequence.
/// @returns The joined string.
template <typename Iterator, typename Predicate>
std::string join(Iterator begin, Iterator end, std::string const& sep,
                 Predicate p)
{
  std::string result;
  if (begin != end)
    result += p(*begin++);
  while (begin != end)
    result += sep + p(*begin++);
  return result;
}

template <typename Iterator>
std::string join(Iterator begin, Iterator end, std::string const& sep)
{
  return join(begin, end, sep, [](auto&& x) -> decltype(x) { return x; });
}

template <typename T>
std::string join(std::vector<T> const& v, std::string const& sep)
{
  return join(v.begin(), v.end(), sep);
}

/// Determines whether a string occurs at the beginning of another.
/// @param begin The beginning of the string.
/// @param end The end of the string.
/// @param str The substring to check at the start of *[begin, end)*.
/// @returns `true` iff *str* occurs at the beginning of *[begin, end)*.
template <typename Iterator>
bool starts_with(Iterator begin, Iterator end, std::string const& str)
{
  using diff = typename std::iterator_traits<Iterator>::difference_type;
  if (static_cast<diff>(str.size()) > end - begin)
    return false;
  return std::equal(str.begin(), str.end(), begin);
}

inline bool starts_with(std::string const& str, std::string const& start)
{
  return starts_with(str.begin(), str.end(), start);
}

/// Determines whether a string occurs at the end of another.
/// @param begin The beginning of the string.
/// @param end The end of the string.
/// @param str The substring to check at the end of *[begin, end)*.
/// @returns `true` iff *str* occurs at the end of *[begin, end)*.
template <typename Iterator>
bool ends_with(Iterator begin, Iterator end, std::string const& str)
{
  using diff = typename std::iterator_traits<Iterator>::difference_type;
  return static_cast<diff>(str.size()) <= end - begin
    && std::equal(str.begin(), str.end(), end - str.size());
}

inline bool ends_with(std::string const& str, std::string const& end)
{
  return ends_with(str.begin(), str.end(), end);
}

} // namespace util
} // namespace vast

#endif
