#ifndef VAST_UTIL_SEARCH_H
#define VAST_UTIL_SEARCH_H

#include <cassert>
#include <algorithm>
#include <array>
#include <iterator>
#include <limits>
#include <unordered_map>
#include <type_traits>
#include <vector>

namespace vast {
namespace util {

namespace detail {

template <typename Key, typename Value>
class unordered_skip_table
{
  static_assert(sizeof(Key) > 1,
                "unordered skip table makes only sense for large keys");

public:
  unordered_skip_table(size_t n, Value default_value)
      : default_{default_value},
        skip_(n)
  {
  }

  void insert(Key key, Value value)
  {
    skip_[key] = value;
  }

  Value operator[](Key const& key) const
  {
    auto i = skip_.find(key);
    return i == skip_.end () ? default_ : i->second;
  }

private:
  std::unordered_map<Key, Value> skip_;
  Value const default_;
};

template <typename Key, typename Value>
class array_skip_table
{
  static_assert(std::is_integral<Key>::value,
                "array skip table key must be integral");

  static_assert(sizeof(Key) == 1,
                "array skip table key must occupy one byte");

public:
  array_skip_table(size_t, Value default_value)
  {
    std::fill_n(skip_.begin(), skip_.size(), default_value);
  }

  void insert(Key key, Value val)
  {
    skip_[static_cast<unsigned_key_type>(key)] = val;
  }

  Value operator[](Key key) const
  {
    return skip_[static_cast<unsigned_key_type>(key)];
  }

private:
  using unsigned_key_type = typename std::make_unsigned<Key>::type;
  std::array<Value, std::numeric_limits<unsigned_key_type>::max()> skip_;
};

} // namespace detail

/// A stateful [Boyer-Moore](http://bit.ly/boyer-moore-pub) search context. It
/// can look for a pattern *P* over a (text) sequence *T*.
/// @tparam PatternIterator The iterator type over the pattern.
template <typename PatternIterator>
class boyer_moore
{
  template <typename Container>
  static void make_prefix(PatternIterator begin, PatternIterator end,
                          Container &pfx)
  {
    assert(end - begin > 0);
    assert(pfx.size() == static_cast<size_t>(end - begin));

    pfx[0] = 0;
    size_t k = 0;
    for (decltype(end - begin) i = 1; i < end - begin; ++i)
    {
      while (k > 0 && begin[k] != begin[i])
        k = pfx[k - 1];

      if (begin[k] == begin[i])
        k++;

      pfx[i] = k;
    }
  }

public:
  /// Construct a Boyer-Moore search context from a pattern.
  /// @param begin The start of the pattern.
  /// @param end The end of the pattern.
  boyer_moore(PatternIterator begin, PatternIterator end)
    : pat_{begin},
      n_{end - begin},
      skip_{static_cast<size_t>(n_), -1},
      suffix_(n_ + 1)
  {
    if (n_ == 0)
      return;

    // Build the skip table (delta_1).
    for (decltype(n_) i = 0; i < n_; ++i)
      skip_.insert(pat_[i], i);

    // Build the suffix table (delta2).
    std::vector<pat_char_type> reversed(n_);
    std::reverse_copy(begin, end, reversed.begin());
    decltype(suffix_) prefix(n_);
    decltype(suffix_) prefix_reversed(n_);
    make_prefix(begin, end, prefix);
    make_prefix(reversed.begin(), reversed.end(), prefix_reversed);

    for (size_t i = 0; i < suffix_.size(); i++)
      suffix_[i] = n_ - prefix[n_ - 1];

    for (decltype(n_) i = 0; i < n_; i++)
    {
      auto j = n_ - prefix_reversed[i];
      auto k = i - prefix_reversed[i] + 1;
      if (suffix_[j] > k)
        suffix_[j] = k;
    }
  }

  /// Looks for *P* in *T*.
  /// @tparam TextIterator A random-access iterator over *T*.
  /// @param begin The start of *T*.
  /// @param end The end of *T*
  /// @returns The position in *T* where *P* occurrs or *end* if *P* does not
  ///     exist in *T*.
  template <typename TextIterator>
  TextIterator operator()(TextIterator begin, TextIterator end) const
  {
    /// Empty *P* always matches at the beginning of *T*.
    if (n_ == 0)
      return begin;

    // Empty *T* or |T| < |P| can never match.
    if (begin == end || end - begin < n_)
      return end;

    auto i = begin;
    while (i <= end - n_)
    {
      auto j = n_;
      while (pat_[j - 1] == i[j - 1])
        if (--j == 0)
          return i;

      auto k = skip_[i[j - 1]];
      auto m = j - k - 1;
      i += k < j && m > suffix_[j] ? m : suffix_[j];
    }

    return end;
  }

private:
  using pat_char_type =
    typename std::iterator_traits<PatternIterator>::value_type;

  using pat_difference_type =
    typename std::iterator_traits<PatternIterator>::difference_type;

  using skip_table =
    std::conditional_t<
      std::is_integral<pat_char_type>::value && sizeof(pat_char_type) == 1,
      detail::array_skip_table<pat_char_type, pat_difference_type>,
      detail::unordered_skip_table<pat_char_type, pat_difference_type>
    >;

  PatternIterator pat_;
  pat_difference_type n_;
  skip_table skip_;
  std::vector<pat_difference_type> suffix_;
};

/// Constructs a Boyer-Moore search context from a pattern.
/// @tparam Iterator A random-access iterator for the pattern
/// @param begin The iterator to the start of the pattern.
/// @param end The iterator to the end of the pattern.
template <typename Iterator>
auto make_boyer_moore(Iterator begin, Iterator end)
{
  return boyer_moore<Iterator>{begin, end};
}

/// Performs a [Boyer-Moore](http://bit.ly/boyer-moore-pub) search of a pattern
/// *P* over a sequence *T*.
///
/// @tparam TextIterator A random-access iterator for *T*
/// @tparam PatternIterator A random-access iterator for *P*
/// @param p0 The iterator to the start of *P*.
/// @param p1 The iterator to the end of *P*.
/// @param t0 The iterator to the start of *T*.
/// @param t0 The iterator to the end of *T*.
/// @returns An iterator to the first occurrence of *P* in *T* or *t1* if *P*
///     does not occur in *T*.
template <typename TextIterator, typename PatternIterator>
TextIterator search_boyer_moore(PatternIterator p0, PatternIterator p1,
                                TextIterator t0, TextIterator t1)
{
  return make_boyer_moore(p0, p1)(t0, t1);
}

} // namespace util
} // namespace vast

#endif
