#ifndef VAST_UTIL_FIELD_SPLITTER_H
#define VAST_UTIL_FIELD_SPLITTER_H

#include <algorithm>
#include <cassert>
#include <iterator>
#include <vector>

namespace vast {
namespace util {

/// Splits an iterator range into a sequence of iterator pairs according to a
/// given separator.
template <typename Iterator>
class field_splitter
{
  static_assert(
      std::is_same<
        typename std::iterator_traits<Iterator>::iterator_category,
        std::random_access_iterator_tag
      >::value,
      "field splitter requires random access iterator");

public:
  /// Constructs a field splitter.
  /// @param sep The field separator to use.
  /// @param sep_len The string length of *sep*.
  field_splitter(char const* sep = " ", size_t sep_len = 1)
    : sep_(sep),
      sep_len_(sep_len)
  {
  }

  /// Splits the given range *[start,end)* into fields.
  ///
  /// @param start The first element of the range.
  ///
  /// @param end One element past the last element of the range.
  ///
  /// @param max_fields The maximum number of fields to split. If there
  /// exists more input after the last split operation at position *p*, then
  /// the range *[p, end)* will constitute the final element.
  void split(Iterator start, Iterator end, int max_fields = -1)
  {
    auto begin = start;
    while (start != end)
    {
      while (*start != sep_[0] && start != end)
          ++start;

      if (start == end || --max_fields == 0)
      {
          fields_.emplace_back(begin, end);
          return;
      }

      auto cand_end = start++;
      auto is_end = true;
      for (size_t i = 1; i < sep_len_; ++i)
      {
        if (start == end)
        {
          fields_.emplace_back(begin, end);
          return;
        }
        else if (*start == sep_[i])
        {
          ++start;
        }
        else
        {
          is_end = false;
          break;
        }
      }

      if (is_end)
      {
        fields_.emplace_back(begin, cand_end);
        begin = start;
      }
    }
  }

  /// Retrieves the start position of a given field.
  Iterator start(size_t i) const
  {
    assert(i < fields_.size());
    return fields_[i].first;
  }

  /// Retrieves the end position of a given field.
  Iterator end(size_t i) const
  {
    assert(i < fields_.size());
    return fields_[i].second;
  }

  /// Retrieves the number of fields.
  size_t fields() const
  {
    return fields_.size();
  }

  /// Checks a field against a C string.
  /// @param field The field to look at.
  /// @param str The beginning of the string.
  /// @param str_len The size of the string or 0 for auto-detection.
  /// @pre `field < fields()`
  /// @returns `true` *iff* the string at *field* matches *str*.
  bool equals(size_t field, char const* str, size_t str_len = 0)
  {
    assert(field < fields());
    auto size = str_len == 0 ? std::strlen(str) : str_len;
    if (static_cast<size_t>(std::distance(start(field), end(field))) != size)
      return false;
    return std::equal(start(field), end(field), str);
  }

private:
  typedef std::pair<Iterator, Iterator> iterator_pair;
  std::vector<iterator_pair> fields_;

  char const* sep_;
  size_t sep_len_;
};

} // namespace util
} // namespace vast

#endif
