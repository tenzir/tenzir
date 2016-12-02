#ifndef VAST_TYPE_HPP
#define VAST_TYPE_HPP

#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include <caf/intrusive_ptr.hpp>
#include <caf/ref_counted.hpp>

#include "vast/aliases.hpp"
#include "vast/attribute.hpp"
#include "vast/key.hpp"
#include "vast/maybe.hpp"
#include "vast/none.hpp"
#include "vast/offset.hpp"
#include "vast/operator.hpp"
#include "vast/optional.hpp"
#include "vast/time.hpp"
#include "vast/variant.hpp"

#include "vast/concept/hashable/uhash.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/detail/operators.hpp"
#include "vast/detail/range.hpp"
#include "vast/detail/stack_vector.hpp"

namespace vast {

class address;
class data;
class json;
class pattern;
class port;
class schema;
class subnet;

/// An abstract type for ::data.
class type : detail::totally_ordered<type> {
  friend schema; // pointer tracking

public:
  using hasher = xxhash32;

  /// Default-constructs an invalid type.
  type();

  /// Constructs a type from a concrete type.
  /// @param x The concrete type.
  template <class T, class>
  type(T&& x);

  /// Sets the name of the type.
  /// @param str The new name.
  /// @returns A reference to `*this`.
  type& name(std::string str);

  /// Retrieves the name of the type.
  /// @returns The name of the type.
  std::string const& name() const;

  /// Retrieves the type attributes.
  std::vector<attribute>& attributes();
  std::vector<attribute> const& attributes() const;
  type& attributes(std::initializer_list<attribute> list);

  /// Checks whether the hash digest of two types is equal.
  friend bool operator==(const type& x, const type& y);

  /// Checks whether the hash digest of one type is less than or equal to
  /// another.
  friend bool operator<(const type& x, const type& y);

  template <class Inspector>
  friend auto inspect(Inspector& f, type& t) {
    return f(*t.ptr_);
  }

  friend auto& expose(type& t);

  friend bool convert(type const& t, json& j);

private:
  struct impl;

  template <class Inspector>
  friend auto inspect(Inspector&, impl&);

  caf::intrusive_ptr<impl> ptr_;
};

// -- concrete types ---------------------------------------------------------

/// The base class for all concrete types.
template <class Derived>
class concrete_type
  : detail::totally_ordered<concrete_type<Derived>>,
    detail::totally_ordered<Derived> {
public:
  using base_type = concrete_type<Derived>;

  friend bool operator==(const concrete_type& x, const concrete_type& y) {
    return x.name_ == y.name_ && x.attributes_ == y.attributes_;
  }

  friend bool operator<(const concrete_type& x, const concrete_type& y) {
    return std::tie(x.name_, x.attributes_) < std::tie(y.name_, y.attributes_);
  }

  // To be implemented by derived types.
  friend bool operator==(const Derived& x, const Derived& y);
  friend bool operator<(const Derived& x, const Derived& y);

  template <class Inspector>
  friend auto inspect(Inspector& f, concrete_type& t) {
    return f(t.name_, t.attributes_);
  }

  Derived& name(std::string str) {
    name_ = std::move(str);
    return *static_cast<Derived*>(this);
  }

  std::string const& name() const {
    return name_;
  }

  std::vector<attribute>& attributes() {
    return attributes_;
  }

  std::vector<attribute> const& attributes() const {
    return attributes_;
  }

  Derived& attributes(std::initializer_list<attribute> list) {
    attributes_ = std::move(list);
    return *static_cast<Derived*>(this);
  }

private:
  std::string name_;
  std::vector<attribute> attributes_;
};

/// A type that is fully determined at compile time.
template <class Derived>
struct basic_type : concrete_type<Derived> {
};

#define VAST_DEFINE_DATA_TYPE(T, U)                                           \
struct T : basic_type<T> {                                                    \
  friend bool operator==(T const& x, T const& y) {                            \
    return static_cast<const base_type&>(x) ==                                \
           static_cast<const base_type&>(y);                                  \
  }                                                                           \
                                                                              \
  friend bool operator<(T const& x, T const& y) {                             \
    return static_cast<const base_type&>(x) <                                 \
           static_cast<const base_type&>(y);                                  \
  }                                                                           \
                                                                              \
  template <class Inspector>                                                  \
  friend auto inspect(Inspector& f, T& x) {                                   \
    return f(static_cast<base_type&>(x), caf::meta::type_name(#T));           \
  }                                                                           \
                                                                              \
  using data_type = U;                                                        \
};

/// The invalid type that doesn't represent a valid type.
VAST_DEFINE_DATA_TYPE(none_type, none)

/// A type for true/false data.
VAST_DEFINE_DATA_TYPE(boolean_type, boolean)

/// A type for positive and negative integers.
VAST_DEFINE_DATA_TYPE(integer_type, integer)

/// A type for positive integers.
VAST_DEFINE_DATA_TYPE(count_type, count)

/// A type for floating point numbers.
VAST_DEFINE_DATA_TYPE(real_type, real)

/// A type for time durations.
VAST_DEFINE_DATA_TYPE(interval_type, interval)

/// A type for absolute points in time.
VAST_DEFINE_DATA_TYPE(timestamp_type, timestamp)

/// A string type for sequence of characters.
VAST_DEFINE_DATA_TYPE(string_type, std::string)

/// A type for regular expressions.
VAST_DEFINE_DATA_TYPE(pattern_type, pattern)

/// A type for IP addresses, both v4 and v6.
VAST_DEFINE_DATA_TYPE(address_type, address)

/// A type for IP prefixes.
VAST_DEFINE_DATA_TYPE(subnet_type, subnet)

/// A type for transport-layer ports.
VAST_DEFINE_DATA_TYPE(port_type, port)

/// The base type for types that depend on runtime information.
template <class Derived>
struct complex_type : concrete_type<Derived> {};

/// The base type for recursive type.
template <class Derived>
struct recursive_type : complex_type<Derived> {};

/// The enumeration type consisting of a fixed number of strings.
struct enumeration_type : complex_type<enumeration_type> {
  using data_type = enumeration;

  enumeration_type(std::vector<std::string> fields = {});

  template <class Inspector>
  friend auto inspect(Inspector& f, enumeration_type& e) {
    return f(static_cast<base_type&>(e),
             caf::meta::type_name("enumeration_type"),
             e.fields);
  }

  std::vector<std::string> fields;
};

/// A type representing a sequence of elements.
struct vector_type : recursive_type<vector_type> {
  using data_type = vector;

  vector_type(type t = {});

  template <class Inspector>
  friend auto inspect(Inspector& f, vector_type& t) {
    return f(static_cast<base_type&>(t),
             caf::meta::type_name("vector_type"),
             t.value_type);
  }

  type value_type;
};

/// A type representing a mathematical set.
struct set_type : recursive_type<set_type> {
  using data_type = set;

  set_type(type t = {});

  template <class Inspector>
  friend auto inspect(Inspector& f, set_type& t) {
    return f(static_cast<base_type&>(t),
             caf::meta::type_name("set_type"),
             t.value_type);
  }

  type value_type;
};

/// A type representinng an associative array.
struct table_type : recursive_type<table_type> {
  using data_type = table;

  table_type(type key = {}, type value = {});

  template <class Inspector>
  friend auto inspect(Inspector& f, table_type& t) {
    return f(static_cast<base_type&>(t),
             caf::meta::type_name("table_type"),
             t.key_type, t.value_type);
  }

  type key_type;
  type value_type;
};

/// A field of a record.
struct record_field : detail::totally_ordered<record_field> {
  record_field(std::string name = {}, vast::type type = {});

  friend bool operator==(const record_field& x, const record_field& y);
  friend bool operator<(const record_field& x, const record_field& y);

  template <class Inspector>
  friend auto inspect(Inspector& f, record_field& rf) {
    return f(rf.name, rf.type);
  }

  std::string name;
  vast::type type;
};

/// A sequence of fields, where each fields has a name and a type.
struct record_type : recursive_type<record_type> {
  using data_type = vector;

  /// Enables recursive record iteration.
  class each : public detail::range_facade<each> {
  public:
    struct range_state {
      vast::key key() const;
      size_t depth() const;

      detail::stack_vector<record_field const*, 64> trace;
      vast::offset offset;
    };

    each(record_type const& r);

  private:
    friend detail::range_facade<each>;

    void next();
    bool done() const;
    range_state const& get() const;

    range_state state_;
    detail::stack_vector<record_type const*, 64> records_;
  };

  /// Constructs a record type from a list of fields.
  record_type(std::vector<record_field> fields = {});

  /// Constructs a record type from a list of fields.
  record_type(std::initializer_list<record_field> list);

  /// Attemps to resolve a ::key to an ::offset.
  /// @param k The key to resolve.
  /// @returns The ::offset corresponding to *k*.
  maybe<offset> resolve(key const& k) const;

  /// Attemps to resolve an ::offset to a ::key.
  /// @param o The offset to resolve.
  /// @returns The ::key corresponding to *o*.
  maybe<key> resolve(offset const& o) const;

  /// Finds all offset-key pairs for an *exact* key in this and nested records.
  /// @param k The key to resolve.
  /// @returns The offset-key pairs corresponding to the found *k*.
  std::vector<std::pair<offset, key>> find(key const& k) const;

  /// Finds all offset-key pairs for a *prefix* key in this and nested records.
  /// @param k The key to resolve.
  /// @returns The offset-key pairs corresponding to the found *k*.
  std::vector<std::pair<offset, key>> find_prefix(key const& k) const;

  /// Finds all offset-key pairs for a *suffix* key in this and nested records.
  /// @param k The key to resolve.
  /// @returns The offset-key pairs corresponding to the found *k*.
  std::vector<std::pair<offset, key>> find_suffix(key const& k) const;

  /// Retrieves the type at a given key.
  /// @param k The key to resolve.
  /// @returns The type at key *k* or `nullptr` if *k* doesn't resolve.
  type const* at(key const& k) const;

  /// Retrieves the type at a given offset.
  /// @param o The offset to resolve.
  /// @returns The type at offset *o* or `nullptr` if *o* doesn't resolve.
  type const* at(offset const& o) const;

  template <class Inspector>
  friend auto inspect(Inspector& f, record_type& t) {
    return f(static_cast<base_type&>(t),
             caf::meta::type_name("record_type"),
             t.fields);
  }

  std::vector<record_field> fields;
};

/// Recursively flattens the arguments of a record type.
/// @param rec the record to flatten.
/// @returns The flattened record type.
record_type flatten(record_type const& rec);

type flatten(type const& t);

/// Unflattens a flattened record type.
/// @param rec the record to unflatten.
/// @returns The unflattened record type.
record_type unflatten(record_type const& rec);

type unflatten(type const& t);

/// An alias of another type.
struct alias_type : recursive_type<alias_type> {
  using data_type = std::false_type;

  alias_type(type t = {});

  template <class Inspector>
  friend auto inspect(Inspector& f, alias_type& t) {
    return f(static_cast<base_type&>(t),
             caf::meta::type_name("alias_type"),
             t.value_type);
  }

  type value_type;
};

// -- helpers ----------------------------------------------------------------

/// Given a concrete type, retrieves the corresponding data type.
template <class T>
using type_to_data = typename T::data_type;

/// Checks whether a type is recursive.
/// @param t The type to check.
/// @returns `true` iff *t* contains one or more nested types.
bool is_recursive(const type& t);

/// Checks whether a type is a container type.
/// @param t The type to check.
/// @returns `true` iff *t* is a container type.
bool is_container(const type& t);

/// Checks whether two types are *congruent* to each other, i.e., whether they
/// are *representationally equal*.
/// @param x The first type.
/// @param y The second type.
/// @returns `true` *iff* *x* and *y* are congruent.
bool congruent(type const& x, type const& y);

bool congruent(type const& x, data const& y);

bool congruent(data const& x, type const& y);

/// Checks whether the types of two nodes in a predicate are compatible with
/// each other, i.e., whether operator evaluation for the given types is
/// semantically correct.
/// @note This function assumes the AST has already been normalized with the
///       extractor occurring at the LHS and the value at the RHS.
/// @param lhs The LHS of *op*.
/// @param op The operator under which to compare *lhs* and *rhs*.
/// @param rhs The RHS of *op*.
/// @returns `true` if *lhs* and *rhs* are compatible to each other under *op*.
bool compatible(type const& lhs, relational_operator op, type const& rhs);

bool compatible(type const& lhs, relational_operator op, data const& rhs);

bool compatible(data const& lhs, relational_operator op, type const& rhs);

/// Checks whether data and type fit together (and can form a ::value).
/// @param t The type that describes *d*.
/// @param d The raw data to be checked against *t*.
/// @returns `true` if *t* is a valid type for *d*.
bool type_check(type const& t, data const& d);

/// Default-construct a data instance for a given type.
/// @param t The type to construct ::data from.
/// @returns a default-constructed instance of type *t*.
data construct(type const& t);

// -- implementation details -------------------------------------------------

namespace detail {

using type_variant = variant<
  none_type,
  boolean_type,
  integer_type,
  count_type,
  real_type,
  interval_type,
  timestamp_type,
  string_type,
  pattern_type,
  address_type,
  subnet_type,
  port_type,
  enumeration_type,
  vector_type,
  set_type,
  table_type,
  record_type,
  alias_type
>;

using type_types = typename type_variant::types;

} // namespace detail

struct type::impl : caf::ref_counted, detail::type_variant {
  using detail::type_variant::type_variant;

  template <class Inspector>
  friend auto inspect(Inspector& f, impl& i) {
    return f(static_cast<detail::type_variant&>(i));
  }
};

inline auto& expose(type& t) {
  return static_cast<detail::type_variant&>(*t.ptr_);
}

template <
  class T,
  class = std::enable_if_t<
    detail::contains<std::decay_t<T>, detail::type_types>{}
  >
>
type::type(T&& x) : ptr_{new impl{std::forward<T>(x)}} {
}

} // namespace vast

namespace std {

template <>
struct hash<vast::type> {
  size_t operator()(vast::type const& t) const {
    return vast::uhash<vast::type::hasher>{}(t);
  }
};

} // namespace std

#endif
