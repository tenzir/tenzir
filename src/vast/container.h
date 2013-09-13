#ifndef VAST_CONTAINER_H
#define VAST_CONTAINER_H

#include "vast/fwd.h"
#include "vast/offset.h"
#include "vast/value.h"
#include "vast/util/operators.h"
#include "vast/util/parse.h"
#include "vast/util/print.h"

namespace vast {
namespace detail {

/// A mixin that equips containers with any/all quantifiers.
template <typename Derived>
class enumerable
{
public:
  /// Applies a unary predicate to each value, where records are entered
  /// recursively and not considered as an individual value.
  ///
  /// @param f The predicate to test on the values.
  ///
  /// @return `true` if @a f satisfies at least one value.
  bool any(std::function<bool(value const&)> f) const
  {
    return derived().any_impl(f);
  };

  /// Applies a unary predicate to each argument value, where records are
  /// entered recursively and not considered as an individual value.
  ///
  /// @param f The predicate to test on all values.
  ///
  /// @return `true` if @a f satisfies all values.
  bool all(std::function<bool(value const&)> f) const
  {
    return derived().all_impl(f);
  };

private:
  Derived const& derived() const
  {
    return *static_cast<Derived const*>(this);
  }
};

// Helper class for containers with homogeneous value types. It provides a
// function to ensure that the container value type remains homogeneous.
template <typename Derived>
struct homogeneous
{
  void ensure_type(value_type& type, value const& x)
  {
    if (derived().empty() && type == invalid_type && x.which() != nil_type)
    {
      type = x.which();
      return;
    }

    if (x.which() == nil_type)
      return;

    if (x.which() != type)
      throw error::bad_type("invalid container value type", type, x.which());
  }

  void check_type(value_type type, value const& x) const
  {
    if (derived().empty() && type == invalid_type)
      return;

    if (x.which() == nil_type)
      return;

    if (x.which() != type)
      throw error::bad_type("invalid container value type", type, x.which());
  }

  Derived const& derived() const
  {
    return *static_cast<Derived const*>(this);
  }

  Derived& derived()
  {
    return *static_cast<Derived*>(this);
  }
};

} // namespace detail

class vector : std::vector<value>,
               public detail::enumerable<set>,
               detail::homogeneous<vector>,
               util::totally_ordered<vector>,
               util::printable<vector>
{
  friend detail::homogeneous<vector>;
  friend detail::enumerable<vector>;
  typedef std::vector<value> super;

public:
  // Types
  using super::value_type;
  using super::size_type;
  using super::difference_type;
  using super::iterator;
  using super::const_iterator;
  using super::reference;
  using super::const_reference;

  // Element access
  using super::front;
  using super::back;

  // Iterators
  using super::begin;
  using super::cbegin;
  using super::end;
  using super::cend;
  using super::rbegin;
  using super::crbegin;
  using super::rend;
  using super::crend;

  // Capacity
  using super::empty;
  using super::size;
  using super::max_size;
  using super::capacity;
  using super::reserve;
  using super::shrink_to_fit;

  // Modifiers
  using super::clear;
  using super::resize;

  /// Constructs an empty vector of invalid type.
  vector();

  /// Constructs a vector of a certain type.
  /// @param type The element type.
  vector(vast::value_type type);

  /// Constructs a vector from an initializer list.
  /// @param list The initializer list.
  vector(std::initializer_list<value> list);

  /// Constructs a vector by copying another vector.
  /// @param other The vector to copy.
  vector(vector const& other);

  /// Constructs a vector by moving another vector.
  /// @param other The vector to move.
  vector(vector&& other);

  /// Assigns another vector to this instance.
  /// @param other The right-hand side of the assignment.
  vector& operator=(vector other);

  void insert(iterator i, value x);

  template <typename InputIterator>
  void insert(const_iterator i, InputIterator first, InputIterator last)
  {
    ensure_type(type_, *i);
    super::insert(i, first, last);
  }

  template <typename... Args>
  void emplace(iterator i, Args&&... args)
  {
    value v(std::forward<Args>(args)...);
    ensure_type(type_, v);
    super::insert(i, std::move(v));
  }

  void push_back(value x);

  template <typename... Args>
  void emplace_back(Args&&... args)
  {
    value v(std::forward<Args>(args)...);
    ensure_type(type_, v);
    super::push_back(std::move(v));
  }

  /// Retrieves the value type of the vector.
  /// @return The value type of this vector.
  vast::value_type type() const;

private:
  bool any_impl(std::function<bool(value const&)> f) const;
  bool all_impl(std::function<bool(value const&)> f) const;

  vast::value_type type_;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);
  bool convert(std::string& str) const;

  template <typename Iterator>
  bool print(Iterator& out) const
  {
    *out++ = '[';
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
    *out++ = ']';
    return true;
  }

  friend bool operator==(vector const& x, vector const& y);
  friend bool operator<(vector const& x, vector const& y);
};

/// A set.
class set : std::vector<value>,
            public detail::enumerable<set>,
            detail::homogeneous<set>,
            util::totally_ordered<set>,
            util::parsable<set>,
            util::printable<set>
{
  friend detail::homogeneous<set>;
  friend detail::enumerable<set>;

public:
  typedef std::vector<value> super;

private:
  struct compare
  {
    bool operator()(super::value_type const& v1,
                    super::value_type const& v2) const;
  };

public:
  // Types
  using super::value_type;
  using super::size_type;
  using super::difference_type;
  using super::iterator;
  using super::const_iterator;
  using super::reference;
  using super::const_reference;

  // Element access
  using super::front;
  using super::back;

  // Iterators
  using super::begin;
  using super::cbegin;
  using super::end;
  using super::cend;
  using super::rbegin;
  using super::crbegin;
  using super::rend;
  using super::crend;

  // Capacity
  using super::empty;
  using super::size;
  using super::max_size;
  using super::capacity;
  using super::reserve;
  using super::shrink_to_fit;

  // Modifiers
  using super::clear;
  using super::resize;
  using super::erase;

  /// Constructs an empty set of invalid type.
  set();

  /// Constructs a set of a certain type.
  /// @param type The element type.
  set(vast::value_type type);

  /// Constructs a set from an initializer list.
  /// @param list The initializer list.
  set(std::initializer_list<value> list);

  /// Constructs a set by copying another set.
  /// @param other The set to copy.
  set(set const& other);

  /// Constructs a set by moving another set.
  /// @param other The set to move.
  set(set&& other);

  /// Assigns another set to this instance.
  /// @param other The right-hand side of the assignment.
  set& operator=(set other);

  bool insert(value x);

  template <typename... Args>
  bool emplace(Args&&... args)
  {
    value v(std::forward<Args>(args)...);
    ensure_type(type_, v);
    auto i = find(v);
    if (i == end())
    {
      super::push_back(std::move(v));
      return true;
    }

    if (comp(v, *i))
    {
      super::insert(i, std::move(v));
      return true;
    }

    return false;
  }

  iterator find(value const& x);
  const_iterator find(value const& x) const;

  /// Retrieves the value type of the vector.
  /// @return The value type of this vector.
  vast::value_type type() const;

private:
  bool any_impl(std::function<bool(value const&)> f) const;
  bool all_impl(std::function<bool(value const&)> f) const;

  static compare const comp;
  vast::value_type type_;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  bool parse(Iterator& start,
             Iterator end,
             vast::value_type elem_type,
             string const& sep = ", ",
             string const& esc = "\\")
  {
    if (start == end)
      return false;

    string str;
    auto success = extract(start, end, str);
    if (! success || str.empty())
      return false;

    auto l = str.starts_with("{");
    auto r = str.ends_with("}");
    if (l && r)
      str = str.trim("{", "}");
    else if (l || r)
      return false;

    clear();
    value v;
    for (auto p : str.split(sep, esc))
      if (extract(p.first, p.second, v, elem_type))
        insert(std::move(v));

    return true;
  }

  template <typename Iterator>
  bool print(Iterator& out) const
  {
    *out++ = '{';
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
    *out++ = '}';
    return true;
  }

  friend bool operator==(set const& x, set const& y);
  friend bool operator<(set const& x, set const& y);
};

/// An associative array.
class table : std::vector<std::pair<value, value>>,
              public detail::enumerable<table>,
              detail::homogeneous<table>,
              util::totally_ordered<table>,
              util::printable<table>
{
  friend detail::homogeneous<table>;
  friend detail::enumerable<table>;

public:
  typedef value key_type;
  typedef value mapped_type;
  typedef std::pair<value, value> value_type;
  typedef std::vector<value_type> super;

  // Types
  using super::iterator;
  using super::const_iterator;
  using super::size_type;
  using super::difference_type;

  using super::front;
  using super::back;

  // Iterators
  using super::begin;
  using super::cbegin;
  using super::end;
  using super::cend;

  // Capacity
  using super::empty;
  using super::size;

  // Modifiers
  using super::clear;
  using super::erase;

  struct compare
  {
    bool operator()(key_type const& key1, key_type const& key2) const;
    bool operator()(value_type const& val1, value_type const& val2) const;
    bool operator()(key_type const& key, value_type const& val) const;
    bool operator()(value_type const& val, key_type const& key) const;
  };

  /// Constructs an empty table of invalid type.
  table();

  /// Constructs a table of a certain type.
  /// @param key_type The key type.
  /// @param map_type The mapped type.
  table(vast::value_type key_type, vast::value_type map_type);

  /// Constructs a table from an initializer list.
  /// @param list The initializer list.
  table(std::initializer_list<value> list);

  /// Constructs a table by copying another table.
  /// @param other The table to copy.
  table(table const& other);

  /// Constructs a table by moving another table.
  /// @param other The table to move.
  table(table&& other);

  /// Assigns another table to this instance.
  /// @param other The right-hand side of the assignment.
  table& operator=(table other);

  /// Retrieves the value type of the table key.
  /// @return The value type of the key type.
  vast::value_type key_value_type() const;

  /// Retrieves the value type of the table value.
  /// @return The value type of the mapped type..
  vast::value_type map_value_type() const;

  mapped_type& operator[](const key_type& key);

  std::pair<iterator, bool> insert(value_type v);

  template <typename... Args>
  bool emplace(key_type const& key, Args&&... args)
  {
    ensure_type(key_type_, key);
    value v(std::forward<Args>(args)...);
    ensure_type(map_type_, v);
    value_type element(key, std::move(v));
    auto i = find(key);
    if (i == end())
    {
      super::emplace_back(std::move(element));
      return true;
    }

    if (comp(element, *i))
    {
      super::emplace(i, std::move(element));
      return true;
    }
    return false;
  }

  // Lookup
  iterator find(key_type const& key);
  const_iterator find(key_type const& key) const;

private:
  bool any_impl(std::function<bool(value const&)> f) const;
  bool all_impl(std::function<bool(value const&)> f) const;

  static compare const comp;
  vast::value_type key_type_;
  vast::value_type map_type_;

private:
  friend access;
  friend util::printable<table>;

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

/// A vector of values with arbitrary value types.
class record : public std::vector<value>,
               public detail::enumerable<record>,
               util::totally_ordered<record>,
               util::printable<record>
{
  friend detail::enumerable<record>;

public:
  typedef std::vector<value> super;

  // TODO: remove all constructors and instead inherit them from
  // the base class once gcc implements C++11 inheriting constructors.
  //using std::vector<value>::vector;

  record();
  record(std::vector<value> values);
  record(std::initializer_list<value> list);
  record(record const& other);
  record(record&& other);
  record& operator=(record other);

  /// Recursively accesses a record via a list of offsets serving as indices.
  ///
  /// @param o The list of offset.
  ///
  /// @return A pointer to the value given by *o* or `nullptr` if
  /// *o* does not resolve.
  value const* at(offset const& o) const;

  /// Recursively access a value at a given index.
  ///
  /// @param i The recursive index.
  ///
  /// @return A reference to the value at position @i as if the record was
  /// flattened. Throws an `std::out_of_range` error when @a i exceeds the
  /// flat size of the record.
  value const* flat_at(size_t i) const;

  /// Computes the size of the flat record in *O(n)* time with *n* being the
  /// number of leaf elements in the record..
  ///
  /// @return The size of the flattened record.
  size_t flat_size() const;

  /// Invokes a function on each value, optionally recursing into nested
  /// records.
  ///
  /// @param f The function to invoke on each element.
  ///
  /// @param recurse If `true`, recursively apply *f* to the elements of nested
  /// records.
  void each(std::function<void(value const&)> f, bool recurse = false) const;

private:
  value const* do_flat_at(size_t i, size_t& base) const;
  bool any_impl(std::function<bool(value const&)> f) const;
  bool all_impl(std::function<bool(value const&)> f) const;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

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

} // namespace vast

#endif
