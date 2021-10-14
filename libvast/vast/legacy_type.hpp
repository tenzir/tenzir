//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/aliases.hpp"
#include "vast/attribute.hpp"
#include "vast/concept/hashable/hash.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/concept/printable/print.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/operators.hpp"
#include "vast/detail/range.hpp"
#include "vast/detail/stack_vector.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/offset.hpp"
#include "vast/operator.hpp"
#include "vast/time.hpp"

#include <caf/detail/apply_args.hpp>
#include <caf/detail/int_list.hpp>
#include <caf/detail/type_list.hpp>
#include <caf/error.hpp>
#include <caf/fwd.hpp>
#include <caf/intrusive_cow_ptr.hpp>
#include <caf/make_counted.hpp>
#include <caf/meta/omittable.hpp>
#include <caf/none.hpp>
#include <caf/ref_counted.hpp>
#include <caf/sum_type.hpp>
#include <fmt/core.h>

#include <functional>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

namespace vast {

// -- type hierarchy ----------------------------------------------------------

// clang-format off
/// @relates type
using legacy_concrete_types = caf::detail::type_list<
  legacy_none_type,
  legacy_bool_type,
  legacy_integer_type,
  legacy_count_type,
  legacy_real_type,
  legacy_duration_type,
  legacy_time_type,
  legacy_string_type,
  legacy_pattern_type,
  legacy_address_type,
  legacy_subnet_type,
  legacy_enumeration_type,
  legacy_list_type,
  legacy_map_type,
  legacy_record_type,
  legacy_alias_type
>;
// clang-format on

using type_id_type = int8_t;

constexpr type_id_type invalid_type_id = -1;

template <class T>
constexpr type_id_type type_id() {
  static_assert(detail::contains_type_v<legacy_concrete_types, T>,
                "type IDs only available for concrete types");
  return caf::detail::tl_index_of<legacy_concrete_types, T>::value;
}

// -- type ------------------------------------------------------------------

/// @relates type
using legacy_abstract_type_ptr = caf::intrusive_cow_ptr<legacy_abstract_type>;

/// The sematic representation of data.
class legacy_type : detail::totally_ordered<legacy_type> {
public:
  // -- construction & assignment ---------------------------------------------

  /// Constructs an invalid type.
  legacy_type() noexcept = default;

  /// Constructs a type from a concrete instance.
  /// @tparam T a type that derives from @ref legacy_abstract_type.
  /// @param x An instance of a type.
  template <class T>
    requires(detail::contains_type_v<legacy_concrete_types, T>)
  legacy_type(T x) : ptr_{caf::make_counted<T>(std::move(x))} {
    // nop
  }

  /// Copy-constructs a type.
  legacy_type(const legacy_type& x) = default;

  /// Copy-assigns a type.
  legacy_type& operator=(const legacy_type& x) = default;

  /// Move-constructs a type.
  legacy_type(legacy_type&&) noexcept = default;

  /// Move-assigns a type.
  legacy_type& operator=(legacy_type&&) noexcept = default;

  /// Assigns a type from another instance
  template <class T>
    requires(detail::contains_type_v<legacy_concrete_types, T>)
  legacy_type& operator=(T x) {
    ptr_ = caf::make_counted<T>(std::move(x));
    return *this;
  }

  // -- modifiers -------------------------------------------------------------

  /// Sets the type name.
  /// @param x The new name of the type.
  legacy_type& name(const std::string& x) &;

  /// Sets the type name.
  /// @param x The new name of the type.
  legacy_type name(const std::string& x) &&;

  /// Specifies a list of attributes.
  /// @param xs The list of attributes.
  legacy_type& attributes(std::vector<attribute> xs) &;

  /// Specifies a list of attributes.
  /// @param xs The list of attributes.
  legacy_type attributes(std::vector<attribute> xs) &&;

  /// Inserts a list of attributes, updating already existing keys with new
  /// values.
  /// @param xs The list of attributes.
  legacy_type& update_attributes(std::vector<attribute> xs) &;

  /// Inserts a list of attributes, updating already existing keys with new
  /// values.
  /// @param xs The list of attributes.
  legacy_type update_attributes(std::vector<attribute> xs) &&;

  // -- inspectors ------------------------------------------------------------

  /// Checks whether a type contains a valid type.
  /// @returns `true` iff the type contains an instantiated type.
  explicit operator bool() const;

  /// @returns the name of the type.
  [[nodiscard]] const std::string& name() const;

  /// @returns The attributes of the type.
  [[nodiscard]] const std::vector<attribute>& attributes() const;

  /// @cond PRIVATE

  [[nodiscard]] legacy_abstract_type_ptr ptr() const;

  [[nodiscard]] const legacy_abstract_type* raw_ptr() const noexcept;

  const legacy_abstract_type* operator->() const noexcept;

  const legacy_abstract_type& operator*() const noexcept;

  struct inspect_helper {
    type_id_type& type_tag;
    legacy_type& x;
  };

  /// @endcond

  friend bool operator==(const legacy_type& x, const legacy_type& y);
  friend bool operator<(const legacy_type& x, const legacy_type& y);

private:
  legacy_type(legacy_abstract_type_ptr x);

  legacy_abstract_type_ptr ptr_;
};

/// Describes properties of a type.
/// @relates type
enum class type_flags : uint8_t {
  basic = 0b0000'0001,
  complex = 0b0000'0010,
  recursive = 0b0000'0100,
  container = 0b0000'1000,
};

/// Retrieves kind of a type, e.g., `string` for `legacy_string_type`.
/// @param x The type to query.
/// @param the kind of *x*.
/// @relates type
std::string kind(const legacy_type& x);

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
class legacy_abstract_type : public caf::ref_counted,
                             detail::totally_ordered<legacy_abstract_type> {
  friend legacy_type; // to change name/attributes of a copy.

public:
  virtual ~legacy_abstract_type();

  // -- introspection ---------------------------------------------------------

  friend bool
  operator==(const legacy_abstract_type& x, const legacy_abstract_type& y);
  friend bool
  operator<(const legacy_abstract_type& x, const legacy_abstract_type& y);

  /// @cond PRIVATE

  /// @returns properties of the type.
  virtual type_flags flags() const noexcept = 0;

  /// @returns the index of this type in `legacy_concrete_types`.
  virtual int index() const noexcept = 0;

  virtual legacy_abstract_type* copy() const = 0;

  /// @endcond
  template <class Inspector>
  friend auto inspect(Inspector& f, legacy_abstract_type& x) {
    return f(caf::meta::type_name("vast.abstract_type"),
             caf::meta::omittable_if_empty(), x.name_,
             caf::meta::omittable_if_empty(), x.attributes_);
  }

protected:
  virtual bool equals(const legacy_abstract_type& other) const;

  virtual bool less_than(const legacy_abstract_type& other) const;

  std::string name_;
  std::vector<attribute> attributes_;
};

/// The base class for all concrete types.
/// @relates type
template <class Derived>
class legacy_concrete_type : public legacy_abstract_type,
                             detail::totally_ordered<Derived, Derived> {
public:
  /// @returns the name of the type.
  const std::string& name() const {
    return this->name_;
  }

  /// Sets the type name.
  /// @param x The new name of the type.
  Derived& name(std::string x) & {
    this->name_ = std::move(x);
    return derived();
  }

  /// Sets the type name.
  /// @param x The new name of the type.
  Derived name(const std::string& x) && {
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

  Derived attributes(std::vector<attribute> xs) && {
    this->attributes_ = std::move(xs);
    return std::move(derived());
  }

  Derived& update_attributes(std::vector<attribute> xs) & {
    auto& attrs = this->attributes_;
    for (auto& x : xs) {
      auto i = std::find_if(attrs.begin(), attrs.end(), [&](auto& attr) {
        return attr.key == x.key;
      });
      if (i == attrs.end())
        attrs.push_back(std::move(x));
      else
        i->value = std::move(x).value;
    }
    return derived();
  }

  Derived update_attributes(std::vector<attribute> xs) && {
    auto& attrs = this->attributes_;
    for (auto& x : xs) {
      auto i = std::find_if(attrs.begin(), attrs.end(), [&](auto& attr) {
        return attr.key == x.key;
      });
      if (i == attrs.end())
        attrs.push_back(std::move(x));
      else
        i->value = std::move(x).value;
    }
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
  friend auto inspect(Inspector& f, legacy_concrete_type& x) {
    const char* name = nullptr;
    if constexpr (std::is_same_v<Derived, legacy_none_type>) {
      name = "vast.none_type";
    } else if constexpr (std::is_same_v<Derived, legacy_bool_type>) {
      name = "vast.bool_type";
    } else if constexpr (std::is_same_v<Derived, legacy_integer_type>) {
      name = "vast.integer_type";
    } else if constexpr (std::is_same_v<Derived, legacy_count_type>) {
      name = "vast.count_type";
    } else if constexpr (std::is_same_v<Derived, legacy_real_type>) {
      name = "vast.real_type";
    } else if constexpr (std::is_same_v<Derived, legacy_duration_type>) {
      name = "vast.duration_type";
    } else if constexpr (std::is_same_v<Derived, legacy_time_type>) {
      name = "vast.time_type";
    } else if constexpr (std::is_same_v<Derived, legacy_string_type>) {
      name = "vast.string_type";
    } else if constexpr (std::is_same_v<Derived, legacy_pattern_type>) {
      name = "vast.pattern_type";
    } else if constexpr (std::is_same_v<Derived, legacy_address_type>) {
      name = "vast.address_type";
    } else if constexpr (std::is_same_v<Derived, legacy_subnet_type>) {
      name = "vast.subnet_type";
    } else if constexpr (std::is_same_v<Derived, legacy_enumeration_type>) {
      name = "vast.enumeration_type";
    } else if constexpr (std::is_same_v<Derived, legacy_list_type>) {
      name = "vast.list_type";
    } else if constexpr (std::is_same_v<Derived, legacy_map_type>) {
      name = "vast.map_type";
    } else if constexpr (std::is_same_v<Derived, legacy_record_type>) {
      name = "vast.record_type";
    } else if constexpr (std::is_same_v<Derived, legacy_alias_type>) {
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
  static const Derived& downcast(const legacy_abstract_type& x) {
    VAST_ASSERT(dynamic_cast<const Derived*>(&x) != nullptr);
    return static_cast<const Derived&>(x);
  }

  template <class T>
  static legacy_concrete_type<Derived>& upcast(T& x) {
    return static_cast<legacy_concrete_type&>(x);
  }

  legacy_concrete_type* copy() const final {
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
struct legacy_basic_type : legacy_concrete_type<Derived> {
  [[nodiscard]] type_flags flags() const noexcept final {
    return type_flags::basic;
  }
};

/// The base type for types that depend on runtime information.
/// @relates legacy_basic_type type
template <class Derived>
struct legacy_complex_type : legacy_concrete_type<Derived> {
  [[nodiscard]] type_flags flags() const noexcept override {
    return type_flags::complex | type_flags::recursive;
  }
};

/// The base type for types that contain nested types.
/// @relates type
template <class Derived>
struct legacy_recursive_type : legacy_complex_type<Derived> {
  using super = legacy_complex_type<Derived>;

  [[nodiscard]] type_flags flags() const noexcept override {
    return type_flags::complex | type_flags::recursive;
  }
};

/// The base type for types that a single nested type.
template <class Derived>
struct legacy_nested_type : legacy_recursive_type<Derived> {
  friend class legacy_concrete_type<Derived>; // equals/less_than
  using super = legacy_recursive_type<Derived>;

  explicit legacy_nested_type(legacy_type t = {}) : value_type{std::move(t)} {
    // nop
  }

  legacy_type value_type; ///< The type of the enclosed element(s).

  template <class Inspector>
  friend auto inspect(Inspector& f, Derived& x) {
    return f(caf::meta::type_name("vast.nested_type"), super::upcast(x),
             x.value_type);
  }

  [[nodiscard]] bool equals(const legacy_abstract_type& other) const final {
    return super::equals(other)
           && value_type == super::downcast(other).value_type;
  }

  [[nodiscard]] bool less_than(const legacy_abstract_type& other) const final {
    return super::less_than(other)
           && value_type < super::downcast(other).value_type;
  }
};

// -- leaf types --------------------------------------------------------------

/// Represents a default constructed type.
struct legacy_none_type final : legacy_basic_type<legacy_none_type> {};

/// A type for true/false data.
/// @relates type
struct legacy_bool_type final : legacy_basic_type<legacy_bool_type> {};

/// A type for positive and negative integers.
/// @relates type
struct legacy_integer_type final : legacy_basic_type<legacy_integer_type> {};

/// A type for positive integers.
/// @relates type
struct legacy_count_type final : legacy_basic_type<legacy_count_type> {};

/// A type for floating point numbers.
/// @relates type
struct legacy_real_type final : legacy_basic_type<legacy_real_type> {};

/// A type for time durations.
/// @relates type
struct legacy_duration_type final : legacy_basic_type<legacy_duration_type> {};

/// A type for absolute points in time.
/// @relates type
struct legacy_time_type final : legacy_basic_type<legacy_time_type> {};

/// A string type for sequence of characters.
struct legacy_string_type final : legacy_basic_type<legacy_string_type> {};

/// A type for regular expressions.
/// @relates type
struct legacy_pattern_type final : legacy_basic_type<legacy_pattern_type> {};

/// A type for IP addresses, both v4 and v6.
/// @relates type
struct legacy_address_type final : legacy_basic_type<legacy_address_type> {};

/// A type for IP prefixes.
/// @relates type
struct legacy_subnet_type final : legacy_basic_type<legacy_subnet_type> {};

/// The enumeration type consisting of a fixed number of strings.
/// @relates type
struct legacy_enumeration_type final
  : legacy_complex_type<legacy_enumeration_type> {
  using super = legacy_complex_type<legacy_enumeration_type>;

  explicit legacy_enumeration_type(std::vector<std::string> xs = {})
    : fields{std::move(xs)} {
    // nop
  }

  std::vector<std::string> fields;

  template <class Inspector>
  friend auto inspect(Inspector& f, legacy_enumeration_type& x) {
    return f(caf::meta::type_name("vast.enumeration_type"), super::upcast(x),
             caf::meta::omittable_if_empty(), x.fields);
  }

  bool equals(const legacy_abstract_type& other) const final {
    return super::equals(other) && fields == downcast(other).fields;
  }

  bool less_than(const legacy_abstract_type& other) const final {
    return super::less_than(other) && fields < downcast(other).fields;
  }
};

/// A type representing a sequence of elements.
/// @relates type
struct legacy_list_type final : legacy_nested_type<legacy_list_type> {
  using super = legacy_nested_type<legacy_list_type>;

  using super::super;

  type_flags flags() const noexcept final {
    return super::flags() | type_flags::container;
  }
};

/// A type representinng an associative array.
struct legacy_map_type final : legacy_recursive_type<legacy_map_type> {
  using super = legacy_recursive_type<legacy_map_type>;

  explicit legacy_map_type(legacy_type key = {}, legacy_type value = {})
    : key_type{std::move(key)}, value_type{std::move(value)} {
    // nop
  }

  legacy_type key_type;   ///< The type of the map keys.
  legacy_type value_type; ///< The type of the map values.

  type_flags flags() const noexcept final {
    return super::flags() | type_flags::container;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, legacy_map_type& x) {
    return f(caf::meta::type_name("vast.map_type"), super::upcast(x),
             x.key_type, x.value_type);
  }

  bool equals(const legacy_abstract_type& other) const final {
    return super::equals(other) && key_type == downcast(other).key_type
           && value_type == downcast(other).value_type;
  }

  bool less_than(const legacy_abstract_type& other) const final {
    return super::less_than(other)
           && std::tie(key_type, downcast(other).key_type)
                < std::tie(value_type, downcast(other).value_type);
  }
};

/// A field of a record.
/// @relates legacy_record_type
struct record_field : detail::totally_ordered<record_field> {
  record_field() noexcept = default;

  explicit record_field(std::string name) noexcept : name{std::move(name)} {
    // nop
  }

  record_field(std::string name, vast::legacy_type type) noexcept
    : name{std::move(name)}, type{std::move(type)} {
    // nop
  }

  std::string name;       ///< The name of the field.
  vast::legacy_type type; ///< The type of the field.

  friend bool operator==(const record_field& x, const record_field& y);
  friend bool operator<(const record_field& x, const record_field& y);

  template <class Inspector>
  friend auto inspect(Inspector& f, record_field& x) {
    return f(caf::meta::type_name("vast.record_field"), x.name, x.type);
  }
};

/// A sequence of fields, where each fields has a name and a type.
struct legacy_record_type final : legacy_recursive_type<legacy_record_type> {
  using super = legacy_recursive_type<legacy_record_type>;

  /// Enables recursive record iteration.
  class each : public detail::range_facade<each> {
  public:
    struct range_state {
      [[nodiscard]] std::string key() const;
      [[nodiscard]] const class legacy_type& type() const;
      [[nodiscard]] size_t depth() const;

      detail::stack_vector<const record_field*, 64> trace;
      vast::offset offset;
    };

    each(const legacy_record_type& r);

  private:
    friend detail::range_facade<each>;

    void next();
    [[nodiscard]] bool done() const;
    [[nodiscard]] const range_state& get() const;

    range_state state_;
    detail::stack_vector<const legacy_record_type*, 64> records_;
  };

  legacy_record_type() = default;

  /// Constructs a record type from a list of fields.
  explicit legacy_record_type(std::vector<record_field> xs) noexcept;

  /// Constructs a record type from a list of fields.
  legacy_record_type(std::initializer_list<record_field> xs) noexcept;

  /// Calculates the number of basic types that can be found when traversing the
  /// tree. An faster version of `flatten(*this).fields.size()` or
  /// `auto rng = each{*this}; std::distance(rng.begin(), rng.end())`
  size_t num_leaves() const;

  /// Attemps to resolve a key to an offset.
  /// @param key The key to resolve.
  /// @returns The offset corresponding to *key*.
  std::optional<offset> resolve(std::string_view key) const;

  /// Attemps to resolve an offset to a key.
  /// @param o The offset to resolve.
  /// @returns The key corresponding to *o*.
  std::optional<std::string> resolve(const offset& o) const;

  /// Finds a record field by exact name.
  /// @param field_name The name of the field to lookup.
  /// @returns A pointer to the found field or `nullptr` otherwise.
  /// @warning The returned pointer becomes invalid when adding or removing
  ///          additional fields.
  const record_field* find(std::string_view field_name) const&;
  const record_field* find(std::string_view field_name) && = delete;

  /// Finds all offsets for a *suffix* key in this and nested records.
  /// @param key The key to resolve.
  /// @returns The offsets of fields matching *key*.
  std::vector<offset> find_suffix(std::string_view key) const;

  /// Retrieves the field at a given key.
  /// @param key The key to resolve.
  /// @returns The field at key *key* or `nullptr` if *key* doesn't resolve.
  const record_field* at(std::string_view key) const&;
  const record_field* at(std::string_view key) && = delete;

  /// Retrieves the field at a given offset.
  /// @param o The offset to resolve.
  /// @returns The field at offset *o* or `nullptr` if *o* doesn't resolve.
  const record_field* at(const offset& o) const&;
  const record_field* at(const offset& o) && = delete;

  /// Replaces the field at a given offset with `field`.
  [[nodiscard]] caf::expected<legacy_record_type>
  assign(const offset& o, const record_field& field) const;

  /// Returns the field at the given offset with the full name as if the record
  /// was flattened.
  /// @param o The offset to resolve.
  /// @code{.cpp}
  ///   auto r = legacy_record_type{{"x", legacy_record_type{"y",
  ///   legacy_count_type{}}}}; ASSERT_EQ(r.flat_field_at({0,0}),
  ///   record_field{"x.y", legacy_count_type{}});
  /// @endcode
  std::optional<record_field> flat_field_at(offset o) const;

  /// Converts an offset into an index for the flattened representation.
  /// @param o The offset to resolve.
  std::optional<size_t> flat_index_at(offset o) const;

  /// Converts an index for the flattened representation into an offset.
  /// @param i The index to resolve.
  std::optional<offset> offset_from_index(size_t i) const;

  friend bool
  operator==(const legacy_record_type& x, const legacy_record_type& y);
  friend bool
  operator<(const legacy_record_type& x, const legacy_record_type& y);

  template <class Inspector>
  friend auto inspect(Inspector& f, legacy_record_type& x) {
    return f(caf::meta::type_name("vast.record_type"), upcast(x), x.fields);
  }

  std::vector<record_field> fields;

  bool equals(const legacy_abstract_type& other) const final;

  bool less_than(const legacy_abstract_type& other) const final;
};

/// An alias of another type.
/// @relates type
struct legacy_alias_type final : legacy_nested_type<legacy_alias_type> {
  using super = legacy_nested_type<legacy_alias_type>;
  using super::super;
};

// -- free functions ----------------------------------------------------------

/// Creates a new unnamed legacy_record_type from an arbitrary number of
/// legacy_record_types.
/// @param rs The source records.
/// @returns The combined legacy_record_type.
/// @relates legacy_record_type
template <typename... Rs>
legacy_record_type concat(const Rs&... rs) {
  legacy_record_type result;
  result.fields.reserve((rs.fields.size() + ...));
  (result.fields.insert(result.fields.end(), rs.fields.begin(),
                        rs.fields.end()),
   ...);
  // TODO: This function is missing an integrity check that makes sure the
  // result does not contain multiple fields with the same name.
  // We should also add a differently named version that deduplicates completely
  // identical fields and recurses into nested records under the same field
  // name.
  return result;
}

/// Creates a new unnamed legacy_record_type containing the fields and attribues
/// of lhs and rhs. Errors if a field of the same name but different types is
/// present in both inputs. Errors is the inputs disagree over the value of an
/// attribute with a certain name.
/// @returns The combined legacy_record_type.
/// @relates legacy_record_type
caf::expected<legacy_record_type>
merge(const legacy_record_type& lhs, const legacy_record_type& rhs);

/// @relates priority_merge
enum class merge_policy { prefer_left, prefer_right };

/// Creates a new unnamed legacy_record_type containing the fields and attribues
/// of lhs and rhs. Uses a merge_policy to decide wheter to use a field from lhs
/// or rhs in case of a conflict.
/// @returns The combined legacy_record_type.
/// @relates legacy_record_type merge_policy
legacy_record_type
priority_merge(const legacy_record_type& lhs, const legacy_record_type& rhs,
               merge_policy p);

/// Removes a field from a legacy_record_type by name.
/// @param r The record to mutate.
/// @param path The sequence of keys pointing to the target field.
/// @returns A new type without the target field if it exists in `r`.
/// @pre `!path.empty()`
/// @relates legacy_record_type
std::optional<legacy_record_type>
remove_field(const legacy_record_type& r, std::vector<std::string_view> path);

/// As above, but use an offset instead of a vector of string to specify
/// the field to be removed.
std::optional<legacy_record_type>
remove_field(const legacy_record_type& r, offset o);

/// Recursively flattens the arguments of a record type.
/// @param rec The record to flatten.
/// @returns The flattened record type.
/// @relates legacy_record_type
legacy_record_type flatten(const legacy_record_type& rec);

/// @relates type legacy_record_type
legacy_type flatten(const legacy_type& t);

/// Queries whether `rec` is a flattened record.
/// @relates type legacy_record_type
bool is_flat(const legacy_record_type& rec);

/// Queries whether `rec` is a flattened record.
/// @relates type legacy_record_type
bool is_flat(const legacy_type& t);

/// Computes the size of a flat representation of `rec`.
size_t flat_size(const legacy_record_type& rec);

/// Computes the size of a flat representation of `rec`.
size_t flat_size(const legacy_type&);

// -- helpers ----------------------------------------------------------------

/// Maps a concrete type to a corresponding data type.
/// @relates type data
template <class>
struct type_traits {
  using data_type = std::false_type;
};

#define VAST_TYPE_TRAIT(name)                                                  \
  template <>                                                                  \
  struct type_traits<legacy_##name##_type> {                                   \
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
VAST_TYPE_TRAIT(enumeration);
VAST_TYPE_TRAIT(list);
VAST_TYPE_TRAIT(map);
VAST_TYPE_TRAIT(record);

#undef VAST_TYPE_TRAIT

template <>
struct type_traits<vast::legacy_type> {
  using data_type = vast::data;
};

template <>
struct type_traits<legacy_none_type> {
  using data_type = caf::none_t;
};

template <>
struct type_traits<legacy_string_type> {
  using data_type = std::string;
};

/// Retrieves the concrete @ref data type for a given type from the hierarchy.
/// @relates type data type_traits
template <class T>
using type_to_data = typename type_traits<T>::data_type;

/// @returns `true` if *x is a *basic* type.
/// @relates type
bool is_basic(const legacy_type& x);

/// @returns `true` if *x is a *complex* type.
/// @relates type
bool is_complex(const legacy_type& x);

/// @returns `true` if *x is a *recursive* type.
/// @relates type
bool is_recursive(const legacy_type& x);

/// @returns `true` if *x is a *container* type.
/// @relates type
bool is_container(const legacy_type& x);

/// Checks whether two types are *congruent* to each other, i.e., whether they
/// are *representationally equal*.
/// @param x The first type.
/// @param y The second type.
/// @returns `true` *iff* *x* and *y* are congruent.
/// @relates type data
bool congruent(const legacy_type& x, const legacy_type& y);

/// @relates type data
bool congruent(const legacy_type& x, const data& y);

/// @relates type data
bool congruent(const data& x, const legacy_type& y);

/// Replaces all types in `xs` that are congruent to a type in `with`.
/// @param xs Pointers to the types that should get replaced.
/// @param with Schema containing potentially congruent types.
/// @returns an error if two types with the same name are not congruent.
/// @relates type
caf::error replace_if_congruent(std::initializer_list<legacy_type*> xs,
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
bool compatible(const legacy_type& lhs, relational_operator op,
                const legacy_type& rhs);

/// @relates type data
bool compatible(const legacy_type& lhs, relational_operator op,
                const data& rhs);

/// @relates type data
bool compatible(const data& lhs, relational_operator op,
                const legacy_type& rhs);

/// Checks whether a type is a subset of another, i.e., whether all fields of
/// the first type are contained within the second type.
/// @param x The first type.
/// @param y The second type.
/// @returns `true` *iff* *x* is a subset of *y*.
bool is_subset(const legacy_type& x, const legacy_type& y);

/// Checks whether data and type fit together.
/// @param t The type that describes *d*.
/// @param x The data to be checked against *t*.
/// @returns `true` if *t* is a valid type for *x*.
bool type_check(const legacy_type& t, const data& x);

// TODO: move to the more idiomatic factory data::make(const legacy_type& t).
/// Default-construct a data instance for a given type.
/// @param t The type to construct ::data from.
/// @returns a default-constructed instance of type *t*.
/// @relates type data
data construct(const legacy_type& t);

/// @returns a digest ID for `x`.
/// @relates type
std::string to_digest(const legacy_type& x);

/// Tries to locate an attribute.
/// @param t The type to check.
/// @param key The attribute key.
/// @returns A pointer to the first found attribute *key*.
/// @relates type
/// @see has_attribute
template <class Type>
const attribute* find_attribute(const Type& t, std::string_view key) {
  auto pred = [&](auto& attr) {
    return attr.key == key;
  };
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
bool has_skip_attribute(const legacy_type& t);

/// @relates type
bool convert(const legacy_type& t, data& d);

} // namespace vast

namespace caf {

template <class Result, class Dispatcher, class T>
auto make_dispatch_fun() {
  using fun = Result (*)(Dispatcher*, const vast::legacy_abstract_type&);
  auto lambda = [](Dispatcher* d, const vast::legacy_abstract_type& ref) {
    return d->invoke(static_cast<const T&>(ref));
  };
  return static_cast<fun>(lambda);
}

template <>
struct sum_type_access<vast::legacy_type> {
  using types = vast::legacy_concrete_types;

  using type0 = vast::legacy_none_type;

  static constexpr bool specialized = true;

  template <class T, int Pos>
  static bool is(const vast::legacy_type& x, sum_type_token<T, Pos>) {
    return x->index() == Pos;
  }

  template <class T, int Pos>
  static const T& get(const vast::legacy_type& x, sum_type_token<T, Pos>) {
    return static_cast<const T&>(*x);
  }

  // NOTE: This overload for get_if breaks the shared abstract type semantics.
  // This was noticed when the content of the type registry was mutated without
  // it's knowledge when type operations on layouts are in play. The exact bug
  // is not clear yet, so we'll refrain from using mutations on type value and
  // construct new ones instead.
  // template <class T, int Pos>
  // static T* get_if(vast::legacy_type* x, sum_type_token<T, Pos>) {
  //  x->ptr().unshare();
  //  auto ptr = x->raw_ptr();
  //  return ptr->index() == Pos ? const_cast<T*>(static_cast<const T*>(ptr))
  //                             : nullptr;
  //}

  template <class T, int Pos>
  static const T* get_if(const vast::legacy_type* x, sum_type_token<T, Pos>) {
    auto ptr = x->raw_ptr();
    return ptr->index() == Pos ? static_cast<const T*>(ptr) : nullptr;
  }

  template <class Result, class Visitor, class... Ts>
  struct dispatcher {
    using const_reference = const vast::legacy_abstract_type&;
    template <class... Us>
    Result dispatch(const_reference x, caf::detail::type_list<Us...>) {
      using fun = Result (*)(dispatcher*, const_reference);
      static fun tbl[] = {make_dispatch_fun<Result, dispatcher, Us>()...};
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
  static Result apply(const vast::legacy_type& x, Visitor&& v, Ts&&... xs) {
    dispatcher<Result, decltype(v), decltype(xs)...> d{
      v, std::forward_as_tuple(xs...)};
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
  using fun = typename Inspector::result_type (*)(Inspector&, legacy_type&);
  auto lambda = [](Inspector& g, legacy_type& ref) {
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
  return [](Inspector& f, legacy_type::inspect_helper& x) -> caf::error {
    using result_type = typename Inspector::result_type;
    if constexpr (Inspector::reads_state) {
      if (x.type_tag != invalid_type_id)
        caf::visit(f, x.x);
      return caf::none;
    } else {
      using reference = legacy_type&;
      using fun = result_type (*)(Inspector&, reference);
      static fun tbl[] = {make_inspect_fun<Inspector, Ts>()...};
      if (x.type_tag != invalid_type_id)
        return tbl[x.type_tag](f, x.x);
      x.x = legacy_type{};
      return caf::none;
    }
  };
}

/// @relates type
template <class Inspector>
auto inspect(Inspector& f, legacy_type::inspect_helper& x) {
  auto g = make_inspect<Inspector>(legacy_concrete_types{});
  return g(f, x);
}

/// @relates type
template <class Inspector>
auto inspect(Inspector& f, legacy_type& x) {
  // We use a single byte for the type index on the wire.
  auto type_tag = x ? static_cast<type_id_type>(x->index()) : invalid_type_id;
  legacy_type::inspect_helper helper{type_tag, x};
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
      return vast::hash<vast::xxh64>(x);                                       \
    }                                                                          \
  }

VAST_DEFINE_HASH_SPECIALIZATION(legacy_type);
VAST_DEFINE_HASH_SPECIALIZATION(legacy_none_type);
VAST_DEFINE_HASH_SPECIALIZATION(legacy_bool_type);
VAST_DEFINE_HASH_SPECIALIZATION(legacy_integer_type);
VAST_DEFINE_HASH_SPECIALIZATION(legacy_count_type);
VAST_DEFINE_HASH_SPECIALIZATION(legacy_real_type);
VAST_DEFINE_HASH_SPECIALIZATION(legacy_duration_type);
VAST_DEFINE_HASH_SPECIALIZATION(legacy_time_type);
VAST_DEFINE_HASH_SPECIALIZATION(legacy_string_type);
VAST_DEFINE_HASH_SPECIALIZATION(legacy_pattern_type);
VAST_DEFINE_HASH_SPECIALIZATION(legacy_address_type);
VAST_DEFINE_HASH_SPECIALIZATION(legacy_subnet_type);
VAST_DEFINE_HASH_SPECIALIZATION(legacy_enumeration_type);
VAST_DEFINE_HASH_SPECIALIZATION(legacy_list_type);
VAST_DEFINE_HASH_SPECIALIZATION(legacy_map_type);
VAST_DEFINE_HASH_SPECIALIZATION(record_field);
VAST_DEFINE_HASH_SPECIALIZATION(legacy_record_type);
VAST_DEFINE_HASH_SPECIALIZATION(legacy_alias_type);

#undef VAST_DEFINE_HASH_SPECIALIZATION

} // namespace std

#include "vast/concept/printable/vast/legacy_type.hpp"

namespace fmt {

template <>
struct formatter<vast::legacy_type> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::legacy_type& value, FormatContext& ctx) {
    auto out = ctx.out();
    vast::print(out, value);
    return out;
  }
};

template <>
struct formatter<vast::legacy_none_type> : formatter<vast::legacy_type> {};

template <>
struct formatter<vast::legacy_bool_type> : formatter<vast::legacy_type> {};

template <>
struct formatter<vast::legacy_integer_type> : formatter<vast::legacy_type> {};

template <>
struct formatter<vast::legacy_count_type> : formatter<vast::legacy_type> {};

template <>
struct formatter<vast::legacy_real_type> : formatter<vast::legacy_type> {};

template <>
struct formatter<vast::legacy_duration_type> : formatter<vast::legacy_type> {};

template <>
struct formatter<vast::legacy_time_type> : formatter<vast::legacy_type> {};

template <>
struct formatter<vast::legacy_string_type> : formatter<vast::legacy_type> {};

template <>
struct formatter<vast::legacy_pattern_type> : formatter<vast::legacy_type> {};

template <>
struct formatter<vast::legacy_address_type> : formatter<vast::legacy_type> {};

template <>
struct formatter<vast::legacy_subnet_type> : formatter<vast::legacy_type> {};

template <>
struct formatter<vast::legacy_enumeration_type> : formatter<vast::legacy_type> {
};

template <>
struct formatter<vast::legacy_list_type> : formatter<vast::legacy_type> {};

template <>
struct formatter<vast::legacy_map_type> : formatter<vast::legacy_type> {};

template <>
struct formatter<vast::legacy_record_type> : formatter<vast::legacy_type> {};

template <>
struct formatter<vast::legacy_alias_type> : formatter<vast::legacy_type> {};

template <>
struct formatter<vast::record_field> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::record_field& value, FormatContext& ctx) {
    auto out = ctx.out();
    vast::print(out, value);
    return out;
  }
};

} // namespace fmt
