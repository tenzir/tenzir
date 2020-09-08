/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/aliases.hpp"
#include "vast/attribute.hpp"
#include "vast/concept/hashable/uhash.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/operators.hpp"
#include "vast/detail/range.hpp"
#include "vast/detail/stack_vector.hpp"
#include "vast/fwd.hpp"
#include "vast/offset.hpp"
#include "vast/operator.hpp"
#include "vast/optional.hpp"
#include "vast/time.hpp"

#include <caf/detail/type_list.hpp>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/fwd.hpp>
#include <caf/intrusive_cow_ptr.hpp>
#include <caf/make_counted.hpp>
#include <caf/meta/omittable.hpp>
#include <caf/none.hpp>
#include <caf/ref_counted.hpp>
#include <caf/variant.hpp>

#include <functional>
#include <string>
#include <type_traits>
#include <vector>

namespace vast {

// -- type hierarchy ----------------------------------------------------------

// clang-format off
/// @relates type
using concrete_types = caf::detail::type_list<
  none_type,
  bool_type,
  integer_type,
  count_type,
  real_type,
  duration_type,
  time_type,
  string_type,
  pattern_type,
  address_type,
  subnet_type,
  port_type,
  enumeration_type,
  list_type,
  map_type,
  record_type,
  alias_type
>;
// clang-format on

using type_id_type = int8_t;

constexpr type_id_type invalid_type_id = -1;

template <class T>
constexpr type_id_type type_id() {
  static_assert(caf::detail::tl_contains<concrete_types, T>::value,
                "type IDs only available for concrete types");
  return caf::detail::tl_index_of<concrete_types, T>::value;
}

// -- type ------------------------------------------------------------------

/// @relates type
using type_digest = xxhash64::result_type;

/// @relates type
using abstract_type_ptr = caf::intrusive_cow_ptr<abstract_type>;

/// The sematic representation of data.
class type : detail::totally_ordered<type> {
public:
  // -- construction & assignment ---------------------------------------------

  /// Constructs an invalid type.
  type() = default;

  /// Constructs a type from a concrete instance.
  /// @tparam T a type that derives from @ref abstract_type.
  /// @param x An instance of a type.
  template <
    class T,
    class = std::enable_if_t<caf::detail::tl_contains<concrete_types, T>::value>
  >
  type(T x) : ptr_{caf::make_counted<T>(std::move(x))} {
    // nop
  }

  /// Copy-constructs a type.
  type(const type& x) = default;

  /// Copy-assigns a type.
  type& operator=(const type& x) = default;

  /// Move-constructs a type.
  type(type&&) = default;

  /// Move-assigns a type.
  type& operator=(type&&) = default;

  /// Assigns a type from another instance
  template <class T>
  type& operator=(T x) {
    ptr_ = caf::make_counted<T>(std::move(x));
    return *this;
  }

  // -- modifiers -------------------------------------------------------------

  /// Sets the type name.
  /// @param x The new name of the type.
  type& name(const std::string& x) &;

  /// Sets the type name.
  /// @param x The new name of the type.
  type&& name(const std::string& x) &&;

  /// Specifies a list of attributes.
  /// @param xs The list of attributes.
  type& attributes(std::vector<attribute> xs) &;

  /// Specifies a list of attributes.
  /// @param xs The list of attributes.
  type&& attributes(std::vector<attribute> xs) &&;

  // -- inspectors ------------------------------------------------------------

  /// Checks whether a type contains a valid type.
  /// @returns `true` iff the type contains an instantiated type.
  explicit operator bool() const;

  /// @returns the name of the type.
  const std::string& name() const;

  /// @returns The attributes of the type.
  const std::vector<attribute>& attributes() const;

  /// @cond PRIVATE

  abstract_type_ptr ptr() const;

  const abstract_type* raw_ptr() const noexcept;

  const abstract_type* operator->() const noexcept;

  const abstract_type& operator*() const noexcept;

  struct inspect_helper {
    type_id_type& type_tag;
    type& x;
  };

  /// @endcond

  friend bool operator==(const type& x, const type& y);
  friend bool operator<(const type& x, const type& y);

private:
  type(abstract_type_ptr x);

  abstract_type_ptr ptr_;
};

/// Describes properties of a type.
/// @relates type
enum class type_flags : uint8_t {
  basic =     0b0000'0001,
  complex =   0b0000'0010,
  recursive = 0b0000'0100,
  container = 0b0000'1000,
};

/// Retrieves kind of a type, e.g., `string` for `string_type`.
/// @param x The type to query.
/// @param the kind of *x*.
/// @relates type
std::string kind(const type& x);

/// @relates type_flags
constexpr type_flags operator|(type_flags x, type_flags y) {
  return type_flags(static_cast<uint8_t>(x) | static_cast<uint8_t>(y));
}

/// @relates type_flags
constexpr type_flags operator&(type_flags x, type_flags y) {
  return type_flags(static_cast<uint8_t>(x) & static_cast<uint8_t>(y));
}

/// @relates type_flags
template <type_flags Flags>
constexpr bool is(type_flags x) {
  return (x & Flags) == Flags;
}

/// The abstract base class for all types.
/// @relates type
class abstract_type
  : public caf::ref_counted,
    detail::totally_ordered<abstract_type> {
  friend type; // to change name/attributes of a copy.

public:
  virtual ~abstract_type();

  // -- introspection ---------------------------------------------------------

  friend bool operator==(const abstract_type& x, const abstract_type& y);
  friend bool operator<(const abstract_type& x, const abstract_type& y);

  /// @cond PRIVATE

  /// @returns properties of the type.
  virtual type_flags flags() const noexcept = 0;

  /// @returns the index of this type in `concrete_types`.
  virtual int index() const noexcept = 0;

  virtual abstract_type* copy() const = 0;

  /// @endcond
  template <class Inspector>
  friend auto inspect(Inspector& f, abstract_type& x) {
    return f(caf::meta::type_name("vast.abstract_type"),
             caf::meta::omittable_if_empty(), x.name_,
             caf::meta::omittable_if_empty(), x.attributes_);
  }

protected:
  virtual bool equals(const abstract_type& other) const;

  virtual bool less_than(const abstract_type& other) const;

  std::string name_;
  std::vector<attribute> attributes_;
};

/// The base class for all concrete types.
/// @relates type
template <class Derived>
class concrete_type
  : public abstract_type,
    detail::totally_ordered<Derived, Derived> {
public:
  /// @returns the name of the type.
  const std::string& name() const {
    return this->name_;
  }

  /// Sets the type name.
  /// @param x The new name of the type.
  Derived& name(const std::string& x) & {
    this->name_ = x;
    return derived();
  }

  /// Sets the type name.
  /// @param x The new name of the type.
  Derived&& name(const std::string& x) && {
    this->name_ = x;
    return std::move(derived());
  }

  /// @returns The attributes of the type.
  const std::vector<attribute>& attributes() const {
    return this->attributes_;
  }

  Derived& attributes(std::vector<attribute> xs) & {
    this->attributes_ = std::move(xs);
    return derived();
  }

  Derived&& attributes(std::vector<attribute> xs) && {
    this->attributes_ = std::move(xs);
    return std::move(derived());
  }

  friend bool operator==(const Derived& x, const Derived& y) {
    return x.equals(y);
  }

  friend bool operator<(const Derived& x, const Derived& y) {
    return x.less_than(y);
  }

  int index() const noexcept final {
    return type_id<Derived>();
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, concrete_type& x) {
    const char* name = nullptr;
    if constexpr (std::is_same_v<Derived, none_type>) {
      name = "vast.none_type";
    } else if constexpr (std::is_same_v<Derived, bool_type>) {
      name = "vast.bool_type";
    } else if constexpr (std::is_same_v<Derived, integer_type>) {
      name = "vast.integer_type";
    } else if constexpr (std::is_same_v<Derived, count_type>) {
      name = "vast.count_type";
    } else if constexpr (std::is_same_v<Derived, real_type>) {
      name = "vast.real_type";
    } else if constexpr (std::is_same_v<Derived, duration_type>) {
      name = "vast.duration_type";
    } else if constexpr (std::is_same_v<Derived, time_type>) {
      name = "vast.time_type";
    } else if constexpr (std::is_same_v<Derived, string_type>) {
      name = "vast.string_type";
    } else if constexpr (std::is_same_v<Derived, pattern_type>) {
      name = "vast.pattern_type";
    } else if constexpr (std::is_same_v<Derived, address_type>) {
      name = "vast.address_type";
    } else if constexpr (std::is_same_v<Derived, subnet_type>) {
      name = "vast.subnet_type";
    } else if constexpr (std::is_same_v<Derived, port_type>) {
      name = "vast.port_type";
    } else if constexpr (std::is_same_v<Derived, enumeration_type>) {
      name = "vast.enumeration_type";
    } else if constexpr (std::is_same_v<Derived, list_type>) {
      name = "vast.list_type";
    } else if constexpr (std::is_same_v<Derived, map_type>) {
      name = "vast.map_type";
    } else if constexpr (std::is_same_v<Derived, record_type>) {
      name = "vast.record_type";
    } else if constexpr (std::is_same_v<Derived, alias_type>) {
      name = "vast.alias_type";
    } else {
      static_assert(detail::always_false_v<Derived>, "cannot inspect non-leaf "
                                                     "type");
    }
    VAST_ASSERT(name != nullptr);
    return f(caf::meta::type_name(name), type_id<Derived>(),
             caf::meta::omittable_if_empty(), x.name_,
             caf::meta::omittable_if_empty(), x.attributes_);
  }

protected:
  // Convenience function to cast an abstract type into an instance of this
  // type. Useful in, e.g., the implementation of comparison operators.
  static const Derived& downcast(const abstract_type& x) {
    VAST_ASSERT(dynamic_cast<const Derived*>(&x) != nullptr);
    return static_cast<const Derived&>(x);
  }

  template <class T>
  static concrete_type<Derived>& upcast(T& x) {
    return static_cast<concrete_type&>(x);
  }

  template <class T>
  static const concrete_type<Derived>& upcast(const T& x) {
    return static_cast<const concrete_type&>(x);
  }

  concrete_type* copy() const final {
    return new Derived(derived());
  }

private:
  Derived& derived() {
    return *static_cast<Derived*>(this);
  }

  const Derived& derived() const {
    return *static_cast<const Derived*>(this);
  }
};

/// A type that does not depend on runtime information.
/// @relates type
template <class Derived>
struct basic_type : concrete_type<Derived> {
  type_flags flags() const noexcept final {
    return type_flags::basic;
  }
};

/// The base type for types that depend on runtime information.
/// @relates basic_type type
template <class Derived>
struct complex_type : concrete_type<Derived> {
  type_flags flags() const noexcept override {
    return type_flags::complex | type_flags::recursive;
  }
};

/// The base type for types that contain nested types.
/// @relates type
template <class Derived>
struct recursive_type : complex_type<Derived> {
  using super = complex_type<Derived>;

  type_flags flags() const noexcept override {
    return type_flags::complex | type_flags::recursive;
  }
};

/// The base type for types that a single nested type.
template <class Derived>
struct nested_type : recursive_type<Derived> {
  friend class concrete_type<Derived>; // equals/less_than
  using super = recursive_type<Derived>;

  nested_type(type t = {}) : value_type{std::move(t)} {
    // nop
  }

  type value_type; ///< The type of the list elements.

  template <class Inspector>
  friend auto inspect(Inspector& f, Derived& x) {
    return f(caf::meta::type_name("vast.nested_type"), super::upcast(x),
             x.value_type);
  }

  bool equals(const abstract_type& other) const final {
    return super::equals(other)
           && value_type == super::downcast(other).value_type;
  }

  bool less_than(const abstract_type& other) const final {
    return super::less_than(other)
           && value_type < super::downcast(other).value_type;
  }
};

// -- leaf types --------------------------------------------------------------

/// Represents a default constructed type.
struct none_type final : basic_type<none_type> {};

/// A type for true/false data.
/// @relates type
struct bool_type final : basic_type<bool_type> {};

/// A type for positive and negative integers.
/// @relates type
struct integer_type final : basic_type<integer_type> {};

/// A type for positive integers.
/// @relates type
struct count_type final : basic_type<count_type> {};

/// A type for floating point numbers.
/// @relates type
struct real_type final : basic_type<real_type> {};

/// A type for time durations.
/// @relates type
struct duration_type final : basic_type<duration_type> {};

/// A type for absolute points in time.
/// @relates type
struct time_type final : basic_type<time_type> {};

/// A string type for sequence of characters.
struct string_type final : basic_type<string_type> {};

/// A type for regular expressions.
/// @relates type
struct pattern_type final : basic_type<pattern_type> {};

/// A type for IP addresses, both v4 and v6.
/// @relates type
struct address_type final : basic_type<address_type> {};

/// A type for IP prefixes.
/// @relates type
struct subnet_type final : basic_type<subnet_type> {};

/// A type for transport-layer ports.
/// @relates type
struct port_type final : basic_type<port_type> {};

/// The enumeration type consisting of a fixed number of strings.
/// @relates type
struct enumeration_type final : complex_type<enumeration_type> {
  using super = complex_type<enumeration_type>;

  enumeration_type(std::vector<std::string> xs = {}) : fields{std::move(xs)} {
    // nop
  }

  std::vector<std::string> fields;

  template <class Inspector>
  friend auto inspect(Inspector& f, enumeration_type& x) {
    return f(caf::meta::type_name("vast.enumeration_type"), super::upcast(x),
             caf::meta::omittable_if_empty(), x.fields);
  }

  bool equals(const abstract_type& other) const final {
    return super::equals(other) && fields == downcast(other).fields;
  }

  bool less_than(const abstract_type& other) const final {
    return super::less_than(other) && fields < downcast(other).fields;
  }
};

/// A type representing a sequence of elements.
/// @relates type
struct list_type final : nested_type<list_type> {
  using super = nested_type<list_type>;

  using super::super;

  type_flags flags() const noexcept final {
    return super::flags() | type_flags::container;
  }
};

/// A type representinng an associative array.
struct map_type final : recursive_type<map_type> {
  using super = recursive_type<map_type>;

  map_type(type key = {}, type value = {})
    : key_type{std::move(key)}, value_type{std::move(value)} {
    // nop
  }

  type key_type;    ///< The type of the map keys.
  type value_type;  ///< The type of the map values.

  type_flags flags() const noexcept final {
    return super::flags() | type_flags::container;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, map_type& x) {
    return f(caf::meta::type_name("vast.map_type"), super::upcast(x),
             x.key_type, x.value_type);
  }

  bool equals(const abstract_type& other) const final {
    return super::equals(other)
           && key_type == downcast(other).key_type
           && value_type == downcast(other).value_type;
  }

  bool less_than(const abstract_type& other) const final {
    return super::less_than(other)
           && std::tie(key_type, downcast(other).key_type)
              < std::tie(value_type, downcast(other).value_type);
  }
};

/// A field of a record.
/// @relates record_type
struct record_field : detail::totally_ordered<record_field> {
  record_field(std::string name = {}, vast::type type = {})
    : name{std::move(name)},
      type{std::move(type)} {
    // nop
  }

  std::string name; ///< The name of the field.
  vast::type type;  ///< The type of the field.

  friend bool operator==(const record_field& x, const record_field& y) {
    return x.name == y.name && x.type == y.type;
  }

  friend bool operator<(const record_field& x, const record_field& y) {
    return std::tie(x.name, x.type) < std::tie(y.name, y.type);
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, record_field& x) {
    return f(caf::meta::type_name("vast.record_field"), x.name, x.type);
  }
};

/// A sequence of fields, where each fields has a name and a type.
struct record_type final : recursive_type<record_type> {
  using super = recursive_type<record_type>;

  /// Enables recursive record iteration.
  class each : public detail::range_facade<each> {
  public:
    struct range_state {
      std::string key() const;
      size_t depth() const;

      detail::stack_vector<const record_field*, 64> trace;
      vast::offset offset;
    };

    each(const record_type& r);

  private:
    friend detail::range_facade<each>;

    void next();
    bool done() const;
    const range_state& get() const;

    range_state state_;
    detail::stack_vector<const record_type*, 64> records_;
  };

  /// Constructs a record type from a list of fields.
  record_type(std::vector<record_field> xs = {});

  /// Constructs a record type from a list of fields.
  record_type(std::initializer_list<record_field> xs);

  /// Attemps to resolve a key to an offset.
  /// @param key The key to resolve.
  /// @returns The offset corresponding to *key*.
  caf::optional<offset> resolve(std::string_view key) const;

  /// Attemps to resolve an offset to a key.
  /// @param o The offset to resolve.
  /// @returns The key corresponding to *o*.
  caf::optional<std::string> resolve(const offset& o) const;

  /// Finds a record field by exact name.
  /// @param field_name The name of the field to lookup.
  /// @returns A pointer to the found field or `nullptr` otherwise.
  /// @warning The returned pointer becomes invalid when adding or removing
  ///          additional fields.
  const record_field* find(std::string_view field_name) const;

  /// Finds all offsets for a *suffix* key in this and nested records.
  /// @param key The key to resolve.
  /// @returns The offsets of fields matching *key*.
  std::vector<offset> find_suffix(std::string_view key) const;

  /// Retrieves the type at a given key.
  /// @param key The key to resolve.
  /// @returns The type at key *key* or `nullptr` if *key* doesn't resolve.
  // TODO: return caf::optional<type> instead.
  const type* at(std::string_view key) const;

  /// Retrieves the type at a given offset.
  /// @param o The offset to resolve.
  /// @returns The type at offset *o* or `nullptr` if *o* doesn't resolve.
  // TODO: return caf::optional<type> instead.
  const type* at(const offset& o) const;

  /// Converts an offset into an index for the flattened representation.
  /// @param o The offset to resolve.
  caf::optional<size_t> flat_index_at(offset o) const;

  friend bool operator==(const record_type& x, const record_type& y);
  friend bool operator<(const record_type& x, const record_type& y);

  template <class Inspector>
  friend auto inspect(Inspector& f, record_type& x) {
    return f(caf::meta::type_name("vast.record_type"), upcast(x), x.fields);
  }

  std::vector<record_field> fields;

  bool equals(const abstract_type& other) const final;

  bool less_than(const abstract_type& other) const final;
};

/// An alias of another type.
/// @relates type
struct alias_type final : nested_type<alias_type> {
  using super = nested_type<alias_type>;
  using super::super;
};

// -- free functions ----------------------------------------------------------

/// Creates a new unnamed record_type from an arbitrary number of record_types.
/// @param rs The source records.
/// @returns The combined record_type.
/// @relates record_type
template <typename... Rs>
record_type concat(const Rs&... rs) {
  record_type result;
  result.fields.reserve((rs.fields.size() + ...));
  (result.fields.insert(result.fields.end(), rs.fields.begin(),
                        rs.fields.end()),
   ...);
  return result;
}

/// Recursively flattens the arguments of a record type.
/// @param rec The record to flatten.
/// @returns The flattened record type.
/// @relates record_type
record_type flatten(const record_type& rec);

/// @relates type record_type
type flatten(const type& t);

/// Queries whether `rec` is a flattened record.
/// @relates type record_type
bool is_flat(const record_type& rec);

/// Queries whether `rec` is a flattened record.
/// @relates type record_type
bool is_flat(const type& t);

/// Computes the size of a flat representation of `rec`.
size_t flat_size(const record_type& rec);

/// Computes the size of a flat representation of `rec`.
size_t flat_size(const type&);

/// Unflattens a flattened record type.
/// @param rec the record to unflatten.
/// @returns The unflattened record type.
/// @relates record_type
record_type unflatten(const record_type& rec);

/// @relates type record_type
type unflatten(const type& t);

// -- helpers ----------------------------------------------------------------

/// Maps a concrete type to a corresponding data type.
/// @relates type data
template <class>
struct type_traits {
  using data_type = std::false_type;
};

#define VAST_TYPE_TRAIT(name)                                                  \
  template <>                                                                  \
  struct type_traits<name##_type> {                                            \
    using data_type = name;                                                    \
  }

VAST_TYPE_TRAIT(bool);
VAST_TYPE_TRAIT(integer);
VAST_TYPE_TRAIT(count);
VAST_TYPE_TRAIT(real);
VAST_TYPE_TRAIT(duration);
VAST_TYPE_TRAIT(time);
VAST_TYPE_TRAIT(pattern);
VAST_TYPE_TRAIT(address);
VAST_TYPE_TRAIT(subnet);
VAST_TYPE_TRAIT(port);
VAST_TYPE_TRAIT(enumeration);
VAST_TYPE_TRAIT(list);
VAST_TYPE_TRAIT(map);

#undef VAST_TYPE_TRAIT

template <>
struct type_traits<none_type> {
  using data_type = caf::none_t;
};

template <>
struct type_traits<string_type> {
  using data_type = std::string;
};

/// Retrieves the concrete @ref data type for a given type from the hierarchy.
/// @relates type data type_traits
template <class T>
using type_to_data = typename type_traits<T>::data_type;

/// @returns `true` if *x is a *basic* type.
/// @relates type
bool is_basic(const type& x);

/// @returns `true` if *x is a *complex* type.
/// @relates type
bool is_complex(const type& x);

/// @returns `true` if *x is a *recursive* type.
/// @relates type
bool is_recursive(const type& x);

/// @returns `true` if *x is a *container* type.
/// @relates type
bool is_container(const type& x);

/// Checks whether two types are *congruent* to each other, i.e., whether they
/// are *representationally equal*.
/// @param x The first type.
/// @param y The second type.
/// @returns `true` *iff* *x* and *y* are congruent.
/// @relates type data
bool congruent(const type& x, const type& y);

/// @relates type data
bool congruent(const type& x, const data& y);

/// @relates type data
bool congruent(const data& x, const type& y);

/// Replaces all types in `xs` that are congruent to a type in `with`.
/// @param xs Pointers to the types that should get replaced.
/// @param with Schema containing potentially congruent types.
/// @returns an error if two types with the same name are not congruent.
/// @relates type
caf::error replace_if_congruent(std::initializer_list<type*> xs,
                                const schema& with);

/// Checks whether the types of two nodes in a predicate are compatible with
/// each other, i.e., whether operator evaluation for the given types is
/// semantically correct.
/// @note This function assumes the AST has already been normalized with the
///       extractor occurring at the LHS and the value at the RHS.
/// @param lhs The LHS of *op*.
/// @param op The operator under which to compare *lhs* and *rhs*.
/// @param rhs The RHS of *op*.
/// @returns `true` if *lhs* and *rhs* are compatible to each other under *op*.
/// @relates type data
bool compatible(const type& lhs, relational_operator op, const type& rhs);

/// @relates type data
bool compatible(const type& lhs, relational_operator op, const data& rhs);

/// @relates type data
bool compatible(const data& lhs, relational_operator op, const type& rhs);

/// Checks whether data and type fit together.
/// @param t The type that describes *d*.
/// @param x The data to be checked against *t*.
/// @returns `true` if *t* is a valid type for *x*.
bool type_check(const type& t, const data& x);

// TODO: move to the more idiomatic factory data::make(const type& t).
/// Default-construct a data instance for a given type.
/// @param t The type to construct ::data from.
/// @returns a default-constructed instance of type *t*.
/// @relates type data
data construct(const type& t);

/// @returns a digest ID for `x`.
/// @relates type
std::string to_digest(const type& x);

/// Tries to locate an attribute.
/// @param t The type to check.
/// @param key The attribute key.
/// @returns A pointer to the first found attribute *key*.
/// @relates type
/// @see has_attribute
template <class Type>
const attribute* find_attribute(const Type& t, std::string_view key) {
  auto pred = [&](auto& attr) { return attr.key == key; };
  auto i = std::find_if(t.attributes().begin(), t.attributes().end(), pred);
  return i != t.attributes().end() ? &*i : nullptr;
}

/// Checks whether a given type has an attribute.
/// @param t The type to check.
/// @param key The attribute key.
/// @returns `true` if *t* has an attribute with key *key*.
/// @relates type
/// @see find_attribute
template <class Type>
bool has_attribute(const Type& t, std::string_view key) {
  return find_attribute(t, key) != nullptr;
}

/// Tests whether a type has a "skip" attribute.
/// @relates has_attribute type
inline bool has_skip_attribute(const type& t) {
  return has_attribute(t, "skip");
}

/// @relates type
bool convert(const type& t, json& j);

} // namespace vast

namespace caf {

template <class Result, class Dispatcher, class T>
auto make_dispatch_fun() {
  using fun = Result (*)(Dispatcher*, const vast::abstract_type&);
  auto lambda = [](Dispatcher* d, const vast::abstract_type& ref) {
    return d->invoke(static_cast<const T&>(ref));
  };
  return static_cast<fun>(lambda);
}

template <>
struct sum_type_access<vast::type> {
  using types = vast::concrete_types;

  using type0 = vast::none_type;

  static constexpr bool specialized = true;

  static bool is(const vast::type& x, sum_type_token<vast::none_type, 0>) {
    return !static_cast<bool>(x);
  }

  template <class T, int Pos>
  static bool is(const vast::type& x, sum_type_token<T, Pos>) {
    return x->index() == Pos;
  }

  template <class T, int Pos>
  static const T& get(const vast::type& x, sum_type_token<T, Pos>) {
    return static_cast<const T&>(*x);
  }

  template <class T, int Pos>
  static const T* get_if(const vast::type* x, sum_type_token<T, Pos>) {
    auto ptr = x->raw_ptr();
    return ptr->index() == Pos ? static_cast<const T*>(ptr) : nullptr;
  }

  template <class Result, class Visitor, class... Ts>
  struct dispatcher {
    using const_reference = const vast::abstract_type&;
    template <class... Us>
    Result dispatch(const_reference x, caf::detail::type_list<Us...>) {
      using fun = Result (*)(dispatcher*, const_reference);
      static fun tbl[] = {
        make_dispatch_fun<Result, dispatcher, Us>()...
      };
      return tbl[x.index()](this, x);
    }

    template <class T>
    Result invoke(const T& x) {
      auto is = caf::detail::get_indices(xs_);
      return caf::detail::apply_args_suffxied(v, is, xs_, x);
    }

    Visitor& v;
    std::tuple<Ts&...> xs_;
  };

  template <class Result, class Visitor, class... Ts>
  static Result apply(const vast::type& x, Visitor&& v, Ts&&... xs) {
    dispatcher<Result, decltype(v), decltype(xs)...> d{
      v, std::forward_as_tuple(xs...)
    };
    types token;
    return d.dispatch(*x, token);
  }
};

} // namespace caf

// -- inspect (needs caf::visit first) -----------------------------------------

namespace vast {

/// @private
template <class Inspector, class T>
auto make_inspect_fun() {
  using fun = typename Inspector::result_type (*)(Inspector&, type&);
  auto lambda = [](Inspector& g, type& ref) {
    T tmp;
    auto res = g(caf::meta::type_name("vast.type"), tmp);
    ref = std::move(tmp);
    return res;
  };
  return static_cast<fun>(lambda);
}

/// @private
template <class Inspector, class... Ts>
auto make_inspect(caf::detail::type_list<Ts...>) {
  return [](Inspector& f, type::inspect_helper& x) -> caf::error {
    using result_type = typename Inspector::result_type;
    if constexpr (Inspector::reads_state) {
      if (x.type_tag != invalid_type_id)
        caf::visit(f, x.x);
      return caf::none;
    } else {
      using reference = type&;
      using fun = result_type (*)(Inspector&, reference);
      static fun tbl[] = {
        make_inspect_fun<Inspector, Ts>()...
      };
      if (x.type_tag != invalid_type_id)
        return tbl[x.type_tag](f, x.x);
      x.x = type{};
      return caf::none;
    }
  };
}

/// @relates type
template <class Inspector>
auto inspect(Inspector& f, type::inspect_helper& x) {
  auto g = make_inspect<Inspector>(concrete_types{});
  return g(f, x);
}

/// @relates type
template <class Inspector>
auto inspect(Inspector& f, type& x) {
  // We use a single byte for the type index on the wire.
  auto type_tag = x ? static_cast<type_id_type>(x->index()) : invalid_type_id;
  type::inspect_helper helper{type_tag, x};
  return f(caf::meta::type_name("vast.type"), caf::meta::omittable(), type_tag,
           helper);
}

} // namespace vast

// -- std::hash ----------------------------------------------------------------

namespace std {

#define VAST_DEFINE_HASH_SPECIALIZATION(type)                                  \
  template <>                                                                  \
  struct hash<vast::type> {                                                    \
    size_t operator()(const vast::type& x) const {                             \
      return vast::uhash<vast::xxhash64>{}(x);                                 \
    }                                                                          \
  }

VAST_DEFINE_HASH_SPECIALIZATION(type);
VAST_DEFINE_HASH_SPECIALIZATION(none_type);
VAST_DEFINE_HASH_SPECIALIZATION(bool_type);
VAST_DEFINE_HASH_SPECIALIZATION(integer_type);
VAST_DEFINE_HASH_SPECIALIZATION(count_type);
VAST_DEFINE_HASH_SPECIALIZATION(real_type);
VAST_DEFINE_HASH_SPECIALIZATION(duration_type);
VAST_DEFINE_HASH_SPECIALIZATION(time_type);
VAST_DEFINE_HASH_SPECIALIZATION(string_type);
VAST_DEFINE_HASH_SPECIALIZATION(pattern_type);
VAST_DEFINE_HASH_SPECIALIZATION(address_type);
VAST_DEFINE_HASH_SPECIALIZATION(subnet_type);
VAST_DEFINE_HASH_SPECIALIZATION(port_type);
VAST_DEFINE_HASH_SPECIALIZATION(enumeration_type);
VAST_DEFINE_HASH_SPECIALIZATION(list_type);
VAST_DEFINE_HASH_SPECIALIZATION(map_type);
VAST_DEFINE_HASH_SPECIALIZATION(record_field);
VAST_DEFINE_HASH_SPECIALIZATION(record_type);
VAST_DEFINE_HASH_SPECIALIZATION(alias_type);

#undef VAST_DEFINE_HASH_SPECIALIZATION

} // namespace std
