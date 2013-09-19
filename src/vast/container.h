#ifndef VAST_CONTAINER_H
#define VAST_CONTAINER_H

#include <map>
#include "vast/fwd.h"
#include "vast/offset.h"
#include "vast/value.h"
#include "vast/util/operators.h"
#include "vast/util/parse.h"
#include "vast/util/print.h"

namespace vast {

/// A vector of values with arbitrary value types.
class record : public std::vector<value>,
               util::totally_ordered<record>,
               util::parsable<record>,
               util::printable<record>
{
  using super = std::vector<value>;

public:
  record() = default;

  template <
    typename... Args,
    typename = DisableIfSameOrDerived<record, Args...>
  >
  record(Args&&... args)
    : super(std::forward<Args>(args)...)
  {
  }

  record(std::initializer_list<value> list)
    : super(std::move(list))
  {
  }

  /// Recursively accesses a vector via a list of offsets serving as indices.
  ///
  /// @param o The list of offset.
  ///
  /// @returns A pointer to the value given by *o* or `nullptr` if
  /// *o* does not resolve.
  value const* at(offset const& o) const;

  /// Recursively access a value at a given index.
  ///
  /// @param i The recursive index.
  ///
  /// @returns A pointer to the value at position *i* as if the record was
  /// flattened or `nullptr` if *i* i exceeds the flat size of the record.
  value const* flat_at(size_t i) const;

  /// Computes the size of the flat record in *O(n)* time with *n* being the
  /// number of leaf elements in the record..
  ///
  /// @returns The size of the flattened record.
  size_t flat_size() const;

  void each(std::function<void(value const&)> f, bool recurse = true) const;
  bool any(std::function<bool(value const&)> f, bool recurse = true) const;
  bool all(std::function<bool(value const&)> f, bool recurse = true) const;

private:
  value const* do_flat_at(size_t i, size_t& base) const;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  bool parse(Iterator& start,
             Iterator end,
             vast::value_type elem_type,
             string const& sep = ", ",
             string const& left = "(",
             string const& right = ")",
             string const& esc = "\\")
  {
    if (start == end)
      return false;

    string str;
    auto success = extract(start, end, str);
    if (! success || str.empty())
      return false;

    auto l = str.starts_with(left);
    auto r = str.ends_with(right);
    if (l && r)
      str = str.trim(left, right);
    else if (l || r)
      return false;

    clear();
    value v;
    for (auto p : str.split(sep, esc))
      if (extract(p.first, p.second, v, elem_type))
        push_back(std::move(v));

    return true;
  }

  template <typename Iterator>
  bool print(Iterator& out) const
  {
    *out++ = '(';
    auto first = begin();
    auto last = end();
    while (first != last)
    {
      if (! render(out, *first))
        return false;
      if (++first != last)
      {
        *out++ = ',';
        *out++ = ' ';
      }
    }
    *out++ = ')';
    return true;
  }

  friend bool operator==(record const& x, record const& y);
  friend bool operator<(record const& x, record const& y);
};

/// An associative array.
class table : public std::map<value, value>,
              util::totally_ordered<table>,
              util::printable<table>
{
  using super = std::map<value, value>;

public:
  table() = default;

  template <
    typename... Args,
    typename = DisableIfSameOrDerived<table, Args...>
  >
  table(Args&&... args)
    : super(std::forward<Args>(args)...)
  {
  }

  table(std::initializer_list<std::pair<value const, value>> list)
    : super(std::move(list))
  {
  }

  void each(std::function<void(value const&, value const&)> f) const;
  bool any(std::function<bool(value const&, value const&)> f) const;
  bool all(std::function<bool(value const&, value const&)> f) const;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  bool print(Iterator& out) const
  {
    *out++ = '{';
    auto first = begin();
    auto last = end();
    while (first != last)
    {
      if (! render(out, first->first))
        return false;
      if (! render(out, " -> "))
        return false;
      if (! render(out, first->second))
        return false;
      if (++first != last)
      if (! render(out, ", "))
        return false;
    }
    *out++ = '}';
    return true;
  }

  friend bool operator==(table const& x, table const& y);
  friend bool operator<(table const& x, table const& y);
};

} // namespace vast

#endif
