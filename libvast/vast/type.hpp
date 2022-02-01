//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/aliases.hpp"
#include "vast/chunk.hpp"
#include "vast/concepts.hpp"
#include "vast/detail/generator.hpp"
#include "vast/detail/range.hpp"
#include "vast/detail/stack_vector.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/hash/hash.hpp"
#include "vast/offset.hpp"

#include <caf/detail/apply_args.hpp>
#include <caf/detail/int_list.hpp>
#include <caf/detail/type_list.hpp>
#include <caf/meta/omittable_if_none.hpp>
#include <caf/meta/type_name.hpp>
#include <caf/sum_type.hpp>
#include <fmt/format.h>

#include <compare>
#include <functional>
#include <initializer_list>

namespace vast {

// -- stateful_type_base ------------------------------------------------------

/// The base of type and all concrete complex types.
/// @note This used to be part of `type` itself, and concrete complex types
/// inherited privately from `type` itself. That did, however, cause ADL to
/// sometimes prefer the forbidden conversion to a complex type's private base
/// type `type` over the implicit `type` constructor from complex types.
struct stateful_type_base {
protected:
  /// The underlying representation of the type.
  chunk_ptr table_ = {}; // NOLINT
};

// -- concepts ----------------------------------------------------------------

/// The list of concrete types.
using concrete_types
  = caf::detail::type_list<none_type, bool_type, integer_type, count_type,
                           real_type, duration_type, time_type, string_type,
                           pattern_type, address_type, subnet_type,
                           enumeration_type, list_type, map_type, record_type>;

/// A concept that models any concrete type.
template <class T>
concept concrete_type = requires(const T& value) {
  // The type must be explicitly whitelisted above.
  requires caf::detail::tl_contains<concrete_types, T>::value;
  // The type must not be inherited from to avoid slicing issues.
  requires std::is_final_v<T>;
  // The type must offer a way to get a unique type index.
  {T::type_index};
  // Values of the type must offer an `as_bytes` overload.
  { as_bytes(value) } -> std::same_as<std::span<const std::byte>>;
  // Values of the type must be able to construct the corresponding data type.
  // TODO: Consider including data.hpp and checking whether the returned value
  // is convertible to data.
  {value.construct()};
};

/// A concept that models any concrete type, or the abstract type class itself.
template <class T>
concept type_or_concrete_type
  = std::is_same_v<std::remove_cv_t<T>, type> || concrete_type<T>;

/// A concept that models basic concrete types, i.e., types that do not hold
/// additional state.
template <class T>
concept basic_type = requires {
  // The type must be a concrete type.
  requires concrete_type<T>;
  // The type must not hold any state.
  requires std::is_empty_v<T>;
  // The type must not define any constructors.
  requires std::is_trivial_v<T>;
};

/// A concept that models basic concrete types, i.e., types that hold
/// additional state and extend the lifetime of the surrounding type.
template <class T>
concept complex_type = requires {
  // The type must be a concrete type.
  requires concrete_type<T>;
  // The type must inherit from the same base that `type` inherits from.
  requires std::is_base_of_v<stateful_type_base, T>;
  // The type must only inherit from the same stateful base that `type` inherits
  // from to avoid slicing issues.
  requires sizeof(T) == sizeof(stateful_type_base);
};

/// Maps type to corresponding data.
template <type_or_concrete_type T>
struct type_to_data
  : std::remove_cvref<decltype(std::declval<T>().construct())> {};

/// @copydoc type_to_data
template <type_or_concrete_type T>
using type_to_data_t = typename type_to_data<T>::type;

// -- type --------------------------------------------------------------------

/// The sematic representation of data.
class type final : public stateful_type_base {
public:
  /// An owned key-value type annotation.
  struct attribute final {
    std::string key;                       ///< The key.
    std::optional<std::string> value = {}; ///< The value (optional).
  };

  /// A view on a key-value type annotation.
  struct attribute_view final {
    std::string_view key;   ///< The key.
    std::string_view value; ///< The value (empty if unset).
  };

  /// Indiciates whether to skip over internal types when looking at the
  /// underlying FlatBuffers representation.
  enum class transparent : uint8_t {
    yes, ///< Skip internal types.
    no,  ///< Include internal types. Use with caution.
  };

  /// Default-constructs a type, which is semantically equivalent to the
  /// *none_type*.
  type() noexcept;

  /// Copy-constructs a type, resulting in a shallow copy with shared lifetime.
  /// @param other The copied-from type.
  type(const type& other) noexcept;

  /// Copy-assigns a type, resulting in a shallow copy with shared lifetime.
  /// @param other The copied-from type.
  type& operator=(const type& rhs) noexcept;

  /// Move-constructs a type, leaving the moved-from type in a state
  /// semantically equivalent to the *none_type*.
  /// @param other The moved-from type.
  type(type&& other) noexcept;

  /// Move-constructs a type, leaving the moved-from type in a state
  /// semantically equivalent to the *none_type*.
  /// @param other The moved-from type.
  type& operator=(type&& other) noexcept;

  /// Destroys a type.
  ~type() noexcept;

  /// Constructs a type from an owned sequence of bytes that must contain a
  /// valid `vast.fbs.Type` FlatBuffers root table.
  /// @param table A chunk containing a `vast.fbs.Type` FlatBuffers root table.
  /// @note The table offsets are verified only when assertions are enabled.
  /// @pre `table != nullptr`
  explicit type(chunk_ptr&& table) noexcept;

  /// Explicitly construct a type from a basic concrete type.
  template <basic_type T>
  explicit type(const T& other) noexcept {
    // This creates a chunk that does not own anything, which at first sounds
    // like an antipattern. It is safe for this particular case because the
    // memory for basic types is guaranteed to have static lifetime that
    // exceeds the lifetime of all types.
    table_ = chunk::make(as_bytes(other), []() noexcept {});
  }

  /// Explicitly construct a type from a complex concrete type.
  template <complex_type T>
  explicit type(const T& other) noexcept {
    table_ = other.table_->slice(as_bytes(other));
  }

  /// Constructs a named type with attributes.
  /// @param name The type name.
  /// @param nested The aliased type.
  /// @param attributes The key-value type annotations.
  /// @note Creates a copy of nested if the provided name and attributes are
  /// empty.
  type(std::string_view name, const type& nested,
       const std::vector<struct attribute>& attributes) noexcept;

  template <concrete_type T>
  type(std::string_view name, const T& nested,
       const std::vector<struct attribute>& attributes) noexcept
    : type(name, type{nested}, attributes) {
    // nop
  }

  /// Constructs a named type.
  /// @param name The type name.
  /// @param nested The aliased type.
  /// @note Creates a copy of nested if the provided name is empty.
  type(std::string_view name, const type& nested) noexcept;

  template <concrete_type T>
  type(std::string_view name, const T& nested) noexcept
    : type(name, type{nested}) {
    // nop
  }

  /// Constructs a type with attributes.
  /// @param nested The aliased type.
  /// @param attributes The key-value type annotations.
  /// @note Creates a copy of nested if the attributes are empty.
  type(const type& nested,
       const std::vector<struct attribute>& attributes) noexcept;

  template <concrete_type T>
  type(const T& nested,
       const std::vector<struct attribute>& attributes) noexcept
    : type(type{nested}, attributes) {
    // nop
  }

  /// Infers a type from a given data.
  /// @note Returns a *none_type* if the type cannot be inferred.
  /// @relates data
  [[nodiscard]] static type infer(const data& value) noexcept;

  /// Constructs a type from a legacy_type.
  /// @relates legacy_type
  [[nodiscard]] static type from_legacy_type(const legacy_type& other) noexcept;

  /// Converts a type into a legacy_type.
  /// @note The roundtrip `type::from_legacy_type{x.to_legacy_type()}` will
  /// produce a different type beccause of the inconsistent handling of names
  /// for legacy types. The types will be semantically equivalent, but may not
  /// compare equal.
  /// @relates legacy_type
  [[nodiscard]] legacy_type to_legacy_type() const noexcept;

  /// Returns the underlying FlatBuffers table representation.
  /// @param transparent Whether to skip over internal types.
  [[nodiscard]] const fbs::Type&
  table(enum transparent transparent) const noexcept;

  /// Returns whether the type contains a conrete type other than the *none_type*.
  [[nodiscard]] explicit operator bool() const noexcept;

  /// Compares the underlying representation of two types for equality.
  friend bool operator==(const type& lhs, const type& rhs) noexcept;

  /// Compares the underlying representation of two types lexicographically.
  friend std::strong_ordering
  operator<=>(const type& lhs, const type& rhs) noexcept;

  /// Returns the concrete type index of this type.
  [[nodiscard]] uint8_t type_index() const noexcept;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const type& x) noexcept;
  friend std::span<const std::byte> as_bytes(type&&) noexcept = delete;

  /// Constructs data from the type.
  [[nodiscard]] data construct() const noexcept;

  /// Enables integration with CAF's type inspection.
  template <class Inspector>
  friend auto inspect(Inspector& f, type& x) ->
    typename Inspector::result_type {
    return f(caf::meta::type_name("vast.type"), x.table_);
  }

  /// Integrates with hash_append.
  template <class Hasher>
  friend auto inspect(detail::hash_inspector<Hasher>& f, type& x) ->
    typename detail::hash_inspector<Hasher>::result_type {
    static_assert(detail::hash_inspector<Hasher>::reads_state,
                  "this inspect overload is read-only");
    // Because the underlying table is a chunk_ptr, which cannot be hashed
    // directly, we instead forward the unique representation of it to the hash
    // inspector.
    return f(as_bytes(x));
  }

  /// Integrates with the CAF stringification inspector.
  /// TODO: Implement for all fmt::is_formattable<T, char>.
  friend void inspect(caf::detail::stringification_inspector& f, type& x);

  /// Explicitly forbid usage of the CAF binary serializer/deserializer.
  friend auto inspect(caf::binary_serializer&, type&) = delete;
  friend auto inspect(caf::binary_deserializer&, type&) = delete;

  /// Assigns the metadata of another type to this type.
  void assign_metadata(const type& other) noexcept;

  /// Returns the name of this type.
  /// @note The result is empty if the contained type is unnammed. Built-in
  /// types have no name. Use the {fmt} API to render a type's signature.
  [[nodiscard]] std::string_view name() const& noexcept;
  [[nodiscard]] std::string_view name() && = delete;

  /// Returns a view of all names of this type.
  [[nodiscard]] detail::generator<std::string_view> names() const& noexcept;
  [[nodiscard]] detail::generator<std::string_view> names() && = delete;

  /// Returns a view of all names of this type, alongside all type attributes
  /// that have been defined on the same nesting level as the associated name.
  [[nodiscard]] detail::generator<
    std::pair<std::string_view, std::vector<attribute_view>>>
  names_and_attributes() const& noexcept;

  /// Returns the value of an attribute by name, if it exists.
  /// @param key The key of the attribute.
  /// @note If an attribute exists and its value is empty, the result contains
  /// an empty string to. If the attribute does not exists, the result is
  /// `std::nullopt`.
  // TODO: The generated FlatBuffers code does not work with `std::string_view`.
  // Re-evaluate when upgrading to FlatBuffers 2.0
  [[nodiscard]] std::optional<std::string_view>
  attribute(const char* key) const& noexcept;
  [[nodiscard]] std::optional<std::string_view>
  attribute(const char* key) && = delete;

  /// Returns whether the type has any attributes.
  [[nodiscard]] bool has_attributes() const noexcept;

  /// Returns a view on all attributes.
  [[nodiscard]] detail::generator<attribute_view> attributes() const& noexcept;
  [[nodiscard]] detail::generator<attribute_view> attributes() && = delete;

  /// Returns a flattened type.
  friend type flatten(const type& type) noexcept;

  /// Checks whether a type is a container type.
  friend bool is_container(const type& type) noexcept;

  /// Checks whether two types are *congruent* to each other, i.e., whether they
  /// are *representationally equal*.
  /// @param x The first type or data.
  /// @param y The second type or data.
  friend bool congruent(const type& x, const type& y) noexcept;
  friend bool congruent(const type& x, const data& y) noexcept;
  friend bool congruent(const data& x, const type& y) noexcept;

  /// Checks whether the types of two nodes in a predicate are compatible with
  /// each other, i.e., whether operator evaluation for the given types is
  /// semantically correct.
  /// @note This function assumes the AST has already been normalized with the
  /// extractor occurring at the LHS and the value at the RHS.
  /// @param lhs The LHS of *op*.
  /// @param op The operator under which to compare *lhs* and *rhs*.
  /// @param rhs The RHS of *op*.
  /// @returns `true` if *lhs* and *rhs* are compatible to each other under *op*.
  /// @relates expression
  /// @relates data
  friend bool
  compatible(const type& lhs, relational_operator op, const type& rhs) noexcept;
  friend bool
  compatible(const type& lhs, relational_operator op, const data& rhs) noexcept;
  friend bool
  compatible(const data& lhs, relational_operator op, const type& rhs) noexcept;

  /// Checks whether a type is a subset of another, i.e., whether all fields of
  /// the first type are contained within the second type.
  /// @param x The first type.
  /// @param y The second type.
  /// @returns `true` *iff* *x* is a subset of *y*.
  friend bool is_subset(const type& x, const type& y) noexcept;

  /// Checks whether data and type fit together.
  /// @param x The type that describes the data..
  /// @param y The data to be checked against the type.
  /// @returns `true` if *x* is a valid type for *y*.
  friend bool type_check(const type& x, const data& y) noexcept;
};

/// Compares the underlying representation of two types for equality.
template <type_or_concrete_type T, type_or_concrete_type U>
bool operator==(const T& lhs, const U& rhs) noexcept {
  return type{lhs} == type{rhs};
}

/// Compares the underlying representation of two types lexicographically.
template <type_or_concrete_type T, type_or_concrete_type U>
std::strong_ordering operator<=>(const T& lhs, const U& rhs) noexcept {
  return type{lhs} <=> type{rhs};
}

/// Replaces all types in `xs` that are congruent to a type in `with`.
/// @param xs Pointers to the types that should get replaced.
/// @param with Schema containing potentially congruent types.
/// @returns an error if two types with the same name are not congruent.
/// @relates type
caf::error
replace_if_congruent(std::initializer_list<type*> xs, const schema& with);

// -- none_type ---------------------------------------------------------------

/// Represents a default constructed type.
/// @relates type
class none_type final {
public:
  /// Returns the type index.
  static constexpr uint8_t type_index = 0;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const none_type&) noexcept;

  /// Constructs data from the type.
  [[nodiscard]] static caf::none_t construct() noexcept;
};

// -- bool_type ---------------------------------------------------------------

/// A boolean value that can either be true or false.
/// @relates type
class bool_type final {
public:
  /// Returns the type index.
  static constexpr uint8_t type_index = 1;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const bool_type&) noexcept;

  /// Constructs data from the type.
  [[nodiscard]] static bool construct() noexcept;
};

// -- integer_type ------------------------------------------------------------

/// A signed integer.
/// @relates type
class integer_type final {
public:
  /// Returns the type index.
  static constexpr uint8_t type_index = 2;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const integer_type&) noexcept;

  /// Constructs data from the type.
  [[nodiscard]] static integer construct() noexcept;
};

// -- count_type --------------------------------------------------------------

/// An unsigned integer.
/// @relates type
class count_type final {
public:
  /// Returns the type index.
  static constexpr uint8_t type_index = 3;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const count_type&) noexcept;

  /// Constructs data from the type.
  [[nodiscard]] static count construct() noexcept;
};

// -- real_type ---------------------------------------------------------------

/// A floating-point value.
/// @relates type
class real_type final {
public:
  /// Returns the type index.
  static constexpr uint8_t type_index = 4;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const real_type&) noexcept;

  /// Constructs data from the type.
  [[nodiscard]] static real construct() noexcept;
};

// -- duration_type -----------------------------------------------------------

/// A time interval.
/// @relates type
class duration_type final {
public:
  /// Returns the type index.
  static constexpr uint8_t type_index = 5;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const duration_type&) noexcept;

  /// Constructs data from the type.
  [[nodiscard]] static duration construct() noexcept;
};

// -- time_type ---------------------------------------------------------------

/// A point in time.
/// @relates type
class time_type final {
public:
  /// Returns the type index.
  static constexpr uint8_t type_index = 6;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const time_type&) noexcept;

  /// Constructs data from the type.
  [[nodiscard]] static time construct() noexcept;
};

// -- string_type --------------------------------------------------------------

/// A string of characters.
/// @relates type
class string_type final {
public:
  /// Returns the type index.
  static constexpr uint8_t type_index = 7;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const string_type&) noexcept;

  /// Constructs data from the type.
  [[nodiscard]] static std::string construct() noexcept;
};

// -- pattern_type ------------------------------------------------------------

/// A regular expression.
/// @relates type
class pattern_type final {
public:
  /// Returns the type index.
  static constexpr uint8_t type_index = 8;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const pattern_type&) noexcept;

  /// Constructs data from the type.
  [[nodiscard]] static pattern construct() noexcept;
};

// -- address_type ------------------------------------------------------------

/// An IP address (v4 or v6).
/// @relates type
class address_type final {
public:
  /// Returns the type index.
  static constexpr uint8_t type_index = 9;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const address_type&) noexcept;

  /// Constructs data from the type.
  [[nodiscard]] static address construct() noexcept;
};

// -- subnet_type -------------------------------------------------------------

/// A CIDR subnet.
/// @relates type
class subnet_type final {
public:
  /// Returns the type index.
  static constexpr uint8_t type_index = 10;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const subnet_type&) noexcept;

  /// Constructs data from the type.
  [[nodiscard]] static subnet construct() noexcept;
};

// -- enumeration_type --------------------------------------------------------

/// An enumeration type that can have one specific value.
/// @relates type
class enumeration_type final : public stateful_type_base {
  friend class type;
  friend struct caf::sum_type_access<vast::type>;

public:
  /// A field of an enumeration type.
  struct field final {
    std::string name; ///< The mame of the field.
    uint32_t key = std::numeric_limits<uint32_t>::max(); /// The optional index
                                                         /// of the field.
  };

  /// A view on a field of an enumeration type.
  struct field_view final {
    std::string_view name; ///< The mame of the field.
    uint32_t key = std::numeric_limits<uint32_t>::max(); ///< The optional index
                                                         ///< of the field.
  };

  /// Copy-constructs a type, resulting in a shallow copy with shared lifetime.
  /// @param other The copied-from type.
  enumeration_type(const enumeration_type& other) noexcept;

  /// Copy-assigns a type, resulting in a shallow copy with shared lifetime.
  /// @param other The copied-from type.
  enumeration_type& operator=(const enumeration_type& rhs) noexcept;

  /// Move-constructs a type, leaving the moved-from type in a state
  /// semantically equivalent to the *none_type*.
  /// @param other The moved-from type.
  enumeration_type(enumeration_type&& other) noexcept;

  /// Move-constructs a type, leaving the moved-from type in a state
  /// semantically equivalent to the *none_type*.
  /// @param other The moved-from type.
  enumeration_type& operator=(enumeration_type&& other) noexcept;

  /// Destroys a type.
  ~enumeration_type() noexcept;

  /// Constructs an enumeraton type from a set of fields or field views.
  /// @pre `!fields.empty()`
  explicit enumeration_type(const std::vector<field_view>& fields) noexcept;
  explicit enumeration_type(std::initializer_list<field_view> fields) noexcept;
  explicit enumeration_type(const std::vector<struct field>& fields) noexcept;

  /// Returns the underlying FlatBuffers table representation.
  [[nodiscard]] const fbs::Type& table() const noexcept;

  /// Returns the type index.
  static constexpr uint8_t type_index = 11;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte>
  as_bytes(const enumeration_type& x) noexcept;
  friend std::span<const std::byte>
  as_bytes(enumeration_type&&) noexcept = delete;

  /// Constructs data from the type.
  [[nodiscard]] enumeration construct() const noexcept;

  /// Returns the field at the given key, or an empty string if it does not exist.
  [[nodiscard]] std::string_view field(uint32_t key) const& noexcept;
  [[nodiscard]] std::string_view field(uint32_t key) && = delete;

  /// Returns a view onto all fields, sorted by key.
  [[nodiscard]] std::vector<field_view> fields() const& noexcept;
  [[nodiscard]] std::vector<field_view> fields() && = delete;

  /// Returns the value of the field with the given name, or nullopt if the key
  /// does not exist.
  [[nodiscard]] std::optional<uint32_t>
  resolve(std::string_view key) const noexcept;
};

// -- list_type ---------------------------------------------------------------

/// An ordered sequence of values.
/// @relates type
class list_type final : public stateful_type_base {
  friend class type;
  friend struct caf::sum_type_access<vast::type>;

public:
  /// Copy-constructs a type, resulting in a shallow copy with shared lifetime.
  /// @param other The copied-from type.
  list_type(const list_type& other) noexcept;

  /// Copy-assigns a type, resulting in a shallow copy with shared lifetime.
  /// @param other The copied-from type.
  list_type& operator=(const list_type& rhs) noexcept;

  /// Move-constructs a type, leaving the moved-from type in a state
  /// semantically equivalent to the *none_type*.
  /// @param other The moved-from type.
  list_type(list_type&& other) noexcept;

  /// Move-constructs a type, leaving the moved-from type in a state
  /// semantically equivalent to the *none_type*.
  /// @param other The moved-from type.
  list_type& operator=(list_type&& other) noexcept;

  /// Destroys a type.
  ~list_type() noexcept;

  /// Constructs a list type with a known value type.
  explicit list_type(const type& value_type) noexcept;

  template <concrete_type T>
    requires(!std::is_same_v<T, list_type>) // avoid calling copy constructor
  explicit list_type(const T& value_type) noexcept
    : list_type{type{value_type}} {
    // nop
  }

  /// Returns the underlying FlatBuffers table representation.
  [[nodiscard]] const fbs::Type& table() const noexcept;

  /// Returns the type index.
  static constexpr uint8_t type_index = 12;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const list_type& x) noexcept;
  friend std::span<const std::byte> as_bytes(list_type&&) noexcept = delete;

  /// Constructs data from the type.
  [[nodiscard]] static list construct() noexcept;

  /// Returns the nested value type.
  [[nodiscard]] type value_type() const noexcept;
};

// -- map_type ----------------------------------------------------------------

/// An associative mapping from keys to values.
/// @relates type
class map_type final : public stateful_type_base {
  friend class type;
  friend struct caf::sum_type_access<vast::type>;

public:
  /// Copy-constructs a type, resulting in a shallow copy with shared lifetime.
  /// @param other The copied-from type.
  map_type(const map_type& other) noexcept;

  /// Copy-assigns a type, resulting in a shallow copy with shared lifetime.
  /// @param other The copied-from type.
  map_type& operator=(const map_type& rhs) noexcept;

  /// Move-constructs a type, leaving the moved-from type in a state
  /// semantically equivalent to the *none_type*.
  /// @param other The moved-from type.
  map_type(map_type&& other) noexcept;

  /// Move-constructs a type, leaving the moved-from type in a state
  /// semantically equivalent to the *none_type*.
  /// @param other The moved-from type.
  map_type& operator=(map_type&& other) noexcept;

  /// Destroys a type.
  ~map_type() noexcept;

  /// Constructs a map type with known key and value types.
  explicit map_type(const type& key_type, const type& value_type) noexcept;

  template <type_or_concrete_type T, type_or_concrete_type U>
    requires(!std::is_same_v<T, vast::type> || !std::is_same_v<U, vast::type>)
  explicit map_type(const T& key_type, const U& value_type) noexcept
    : map_type{type{key_type}, type{value_type}} {
    // nop
  }

  /// Returns the underlying FlatBuffers table representation.
  [[nodiscard]] const fbs::Type& table() const noexcept;

  /// Returns the type index.
  static constexpr uint8_t type_index = 13;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const map_type& x) noexcept;
  friend std::span<const std::byte> as_bytes(map_type&&) noexcept = delete;

  /// Constructs data from the type.
  [[nodiscard]] static map construct() noexcept;

  /// Returns the nested key type.
  [[nodiscard]] type key_type() const noexcept;

  /// Returns the nested value type.
  [[nodiscard]] type value_type() const noexcept;
};

// -- record_type -------------------------------------------------------------

/// A list of fields, each of which have a name and type.
/// @relates type
class record_type final : public stateful_type_base {
  friend class type;
  friend struct caf::sum_type_access<vast::type>;

public:
  template <class String>
  struct basic_field {
    basic_field() noexcept = default;
    ~basic_field() noexcept = default;
    basic_field(const basic_field& other) noexcept = default;
    basic_field& operator=(const basic_field& other) noexcept = default;
    basic_field(basic_field&& other) noexcept = default;
    basic_field& operator=(basic_field&& other) noexcept = default;

    template <type_or_concrete_type Type>
    basic_field(String n, const Type& t) noexcept
      : name{std::move(n)}, type{t} {
      // nop
    }

    String name = {};
    class type type = {};
  };

  /// A record type field.
  struct field final : basic_field<std::string> {
    using basic_field::basic_field;
  };

  /// A sliced view on a record type field.
  struct field_view final : basic_field<std::string_view> {
    using basic_field::basic_field;
  };

  /// A sliced view on an indexed leaf field.
  struct leaf_view final {
    field_view field = {}; ///< The leaf field.
    offset index = {};     ///< The leaf field's index.
  };

  /// A transformation that can be applied to a record type; maps a valid offset
  /// to a function that transforms a field into other fields.
  struct transformation {
    using function_type
      = std::function<std::vector<struct field>(const field_view&)>;

    offset index;      ///< The index of the field to transform.
    function_type fun; /// The transformation function to apply.
  };

  /// The behavior of the merge function in case of conflicts.
  enum class merge_conflict {
    fail,         ///< Fail.
    prefer_left,  ///< Take the field from lhs.
    prefer_right, ///< Take the field from rhs.
  };

  /// Copy-constructs a type, resulting in a shallow copy with shared lifetime.
  /// @param other The copied-from type.
  record_type(const record_type& other) noexcept;

  /// Copy-assigns a type, resulting in a shallow copy with shared lifetime.
  /// @param other The copied-from type.
  record_type& operator=(const record_type& rhs) noexcept;

  /// Move-constructs a type, leaving the moved-from type in a state
  /// semantically equivalent to the *none_type*.
  /// @param other The moved-from type.
  record_type(record_type&& other) noexcept;

  /// Move-constructs a type, leaving the moved-from type in a state
  /// semantically equivalent to the *none_type*.
  /// @param other The moved-from type.
  record_type& operator=(record_type&& other) noexcept;

  /// Destroys a type.
  ~record_type() noexcept;

  /// Constructs a record type from a set of fields.
  /// @param fields The ordered fields of the record type.
  /// @pre `!fields.empty()`
  explicit record_type(const std::vector<field_view>& fields) noexcept;
  explicit record_type(std::initializer_list<field_view> fields) noexcept;
  explicit record_type(const std::vector<struct field>& fields) noexcept;

  /// Returns the underlying FlatBuffers table representation.
  [[nodiscard]] const fbs::Type& table() const noexcept;

  /// Returns the type index.
  static constexpr uint8_t type_index = 14;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const record_type& x) noexcept;
  friend std::span<const std::byte> as_bytes(record_type&&) noexcept = delete;

  /// Constructs data from the type.
  [[nodiscard]] record construct() const noexcept;

  /// Returns an iterable view over the fields of a record type.
  [[nodiscard]] detail::generator<field_view> fields() const noexcept;

  /// Returns an iterable view over the leaf fields of a record type.
  [[nodiscard]] detail::generator<leaf_view> leaves() const noexcept;

  /// Returns the numnber of fields in a record.
  [[nodiscard]] size_t num_fields() const noexcept;

  /// Returns the number of leaf fields in a record.
  [[nodiscard]] size_t num_leaves() const noexcept;

  /// Resolves a flat index into an offset.
  [[nodiscard]] offset resolve_flat_index(size_t flat_index) const noexcept;

  /// Resolves a key into an offset.
  /// @note This only matches on full keys, so the key 'x.y'  matches 'x.y.z'
  /// but not 'x.y_other.z' .
  [[nodiscard]] std::optional<offset>
  resolve_key(std::string_view key) const noexcept;

  /// Resolves a key into a list of offsets by suffix matching the given key.
  /// @note This only matches on full keys, so the key 'y.z' matches 'x.y.z' but
  /// not 'x.other_y.z'.
  /// @note The key may optionally begin with a given prefix for backwards
  /// compatilibty with the old type system.
  [[nodiscard]] detail::generator<offset>
  resolve_key_suffix(std::string_view key, std::string_view prefix
                                           = "") const noexcept;

  /// Computes the flattened field name at a given index.
  [[nodiscard]] std::string_view key(size_t index) const& noexcept;
  [[nodiscard]] std::string_view key(size_t index) && = delete;
  [[nodiscard]] std::string key(const offset& index) const noexcept;

  /// Returns the field at the given index.
  [[nodiscard]] field_view field(size_t index) const noexcept;
  [[nodiscard]] field_view field(const offset& index) const noexcept;

  /// Returns the flat index to a given offset.
  /// @note This is necessary to work with the table_slice API, which does not
  /// support direct access via offsets, but rather requires a flat index.
  [[nodiscard]] size_t flat_index(const offset& index) const noexcept;

  /// A transformation that drops fields.
  static transformation::function_type drop() noexcept;

  /// A transformation that replaces a field.
  static transformation::function_type
  assign(std::vector<struct field> fields) noexcept;

  /// A transformation that inserts fields before the index.
  static transformation::function_type
  insert_before(std::vector<struct field> fields) noexcept;

  /// A transformation that inserts fields after the index.
  static transformation::function_type
  insert_after(std::vector<struct field> fields) noexcept;

  /// Creates a new record by applying a set of transformations to this record.
  /// @pre The transformations must be sorted by offset.
  /// @note The changes are applied back-to-front over the individual fields.
  /// This function returns nullopt if the result is empty.
  /// @note While it is possible to apply multiple transformations to the same
  /// field in one go, this may lead to unwanted field duplication.
  /// @pre While this function can operate on non-leaf fields, it requires that
  /// transformations none of the offsets have the same prefix.
  [[nodiscard]] std::optional<record_type>
  transform(std::vector<transformation> transformations) const noexcept;

  /// Creates a new record by merging two records.
  friend caf::expected<record_type>
  merge(const record_type& lhs, const record_type& rhs,
        enum merge_conflict merge_conflict) noexcept;

  /// Returns a new, flattened record type.
  friend record_type flatten(const record_type& type) noexcept;
};

} // namespace vast

// -- misc --------------------------------------------------------------------

namespace vast::fbs {

/// Explicitly poison serialize_bytes for types and concrete types. This was
/// used for the legacy type system, but must not be used for the new one, which
/// has a well-defined contiguous binary representation by design.
template <type_or_concrete_type Type, class Byte = uint8_t>
auto serialize_bytes(flatbuffers::FlatBufferBuilder&, const Type&) = delete;

} // namespace vast::fbs

// -- sum_type_access ---------------------------------------------------------

namespace caf {

template <>
struct sum_type_access<vast::type> final {
  using types = vast::concrete_types;
  using type0 = vast::none_type;
  static constexpr bool specialized = true;

  template <vast::concrete_type T, int Index>
  static bool is(const vast::type& x, sum_type_token<T, Index>) {
    return x.type_index() == T::type_index;
  }

  template <class T, int Index>
  static bool is(const vast::type&, sum_type_token<T, Index>) {
    static_assert(vast::detail::always_false_v<T>, "T must be a concrete type");
    __builtin_unreachable();
  }

  template <vast::basic_type T, int Index>
  static const T& get(const vast::type&, sum_type_token<T, Index>) {
    static const auto instance = T{};
    return instance;
  }

  template <vast::complex_type T, int Index>
  static const T& get(const vast::type& x, sum_type_token<T, Index>) {
    return static_cast<const T&>(
      static_cast<const vast::stateful_type_base&>(x));
  }

  template <class T, int Index>
  static const T& get(const vast::type&, sum_type_token<T, Index>) {
    static_assert(vast::detail::always_false_v<T>, "T must be a concrete type");
    __builtin_unreachable();
  }

  template <vast::concrete_type T, int Index>
  static const T* get_if(const vast::type* x, sum_type_token<T, Index> token) {
    if (x && is(*x, token))
      return &get(*x, token);
    return nullptr;
  }

  template <class T, int Index>
  static const T* get_if(const vast::type*, sum_type_token<T, Index>) {
    static_assert(vast::detail::always_false_v<T>, "T must be a concrete type");
    __builtin_unreachable();
  }

  // A helper function that dispatches from concrete type id to index of the
  // concrete type in the type list. This is intentionally not templatized
  // because it contains a static lookup table that we only ever want to create
  // once.
  static uint8_t index_from_type(const vast::type& x) noexcept;

  template <class Result, class Visitor, class... Args>
  static auto apply(const vast::type& x, Visitor&& v, Args&&... xs) -> Result {
    // A dispatch table that maps variant type index to dispatch function for
    // the concrete type.
    static constexpr auto table =
      []<vast::concrete_type... Ts, uint8_t... Indices>(
        caf::detail::type_list<Ts...>,
        std::integer_sequence<uint8_t, Indices...>) noexcept {
      return std::array{
        +[](const vast::type& x, Visitor&& v, Args&&... xs) -> Result {
          auto xs_as_tuple = std::forward_as_tuple(xs...);
          auto indices = caf::detail::get_indices(xs_as_tuple);
          return caf::detail::apply_args_suffxied(
            std::forward<decltype(v)>(v), std::move(indices), xs_as_tuple,
            get(x, sum_type_token<Ts, Indices>{}));
        }...};
    }
    (types{},
     std::make_integer_sequence<uint8_t, caf::detail::tl_size<types>::value>());
    const auto dispatch = table[index_from_type(x)];
    VAST_ASSERT(dispatch);
    return dispatch(x, std::forward<Visitor>(v), std::forward<Args>(xs)...);
  }
};

} // namespace caf

// -- standard library specializations ----------------------------------------

namespace std {

/// Byte-wise hashing for types.
template <vast::type_or_concrete_type T>
struct hash<T> {
  size_t operator()(const T& type) const noexcept {
    const auto bytes = as_bytes(type);
    return vast::hash(bytes);
  }
};

/// Support transparent key lookup when using type or a concrete type as key in
/// a container.
template <vast::type_or_concrete_type T>
struct equal_to<T> {
  using is_transparent = void; // Opt-in to heterogenous lookups.

  template <vast::type_or_concrete_type Lhs, vast::type_or_concrete_type Rhs>
  constexpr bool operator()(const Lhs& lhs, const Rhs& rhs) const noexcept {
    return lhs == rhs;
  }
};

} // namespace std

// -- formatter ---------------------------------------------------------------

namespace fmt {

template <>
struct formatter<vast::type> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) -> decltype(ctx.begin()) {
    // TODO: n = name_only
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const vast::type& value, FormatContext& ctx)
    -> decltype(ctx.out()) {
    auto out = ctx.out();
    if (const auto& name = value.name(); !name.empty())
      out = format_to(out, "{}", name);
    else
      caf::visit(
        [&](const auto& x) {
          out = format_to(out, "{}", x);
        },
        value);
    for (bool first = false; const auto& attribute : value.attributes()) {
      if (!first) {
        out = format_to(out, " ");
        first = false;
      }
      out = format_to(out, "{}", attribute);
    }
    return out;
  }
};

template <>
struct formatter<vast::type::attribute_view> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) -> decltype(ctx.begin()) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const vast::type::attribute_view& value, FormatContext& ctx)
    -> decltype(ctx.out()) {
    if (value.value.empty())
      return format_to(ctx.out(), "#{}", value.key);
    return format_to(ctx.out(), "#{}={}", value.key, value.value);
  }
};

template <vast::concrete_type T>
struct formatter<T> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) -> decltype(ctx.begin()) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const vast::none_type&, FormatContext& ctx)
    -> decltype(ctx.out()) {
    return format_to(ctx.out(), "none");
  }

  template <class FormatContext>
  auto format(const vast::bool_type&, FormatContext& ctx)
    -> decltype(ctx.out()) {
    return format_to(ctx.out(), "bool");
  }

  template <class FormatContext>
  auto format(const vast::integer_type&, FormatContext& ctx)
    -> decltype(ctx.out()) {
    // TODO: Rename to "integer" when switching to YAML schemas.
    return format_to(ctx.out(), "int");
  }

  template <class FormatContext>
  auto format(const vast::count_type&, FormatContext& ctx)
    -> decltype(ctx.out()) {
    return format_to(ctx.out(), "count");
  }

  template <class FormatContext>
  auto format(const vast::real_type&, FormatContext& ctx)
    -> decltype(ctx.out()) {
    return format_to(ctx.out(), "real");
  }

  template <class FormatContext>
  auto format(const vast::duration_type&, FormatContext& ctx)
    -> decltype(ctx.out()) {
    return format_to(ctx.out(), "duration");
  }

  template <class FormatContext>
  auto format(const vast::time_type&, FormatContext& ctx)
    -> decltype(ctx.out()) {
    return format_to(ctx.out(), "time");
  }

  template <class FormatContext>
  auto format(const vast::string_type&, FormatContext& ctx)
    -> decltype(ctx.out()) {
    return format_to(ctx.out(), "string");
  }

  template <class FormatContext>
  auto format(const vast::pattern_type&, FormatContext& ctx)
    -> decltype(ctx.out()) {
    return format_to(ctx.out(), "pattern");
  }

  template <class FormatContext>
  auto format(const vast::address_type&, FormatContext& ctx)
    -> decltype(ctx.out()) {
    // TODO: Rename to "address" when switching to YAML schemas.
    return format_to(ctx.out(), "addr");
  }

  template <class FormatContext>
  auto format(const vast::subnet_type&, FormatContext& ctx)
    -> decltype(ctx.out()) {
    return format_to(ctx.out(), "subnet");
  }

  template <class FormatContext>
  auto format(const vast::enumeration_type& value, FormatContext& ctx)
    -> decltype(ctx.out()) {
    return format_to(ctx.out(), "enum {{{}}}", fmt::join(value.fields(), ", "));
  }

  template <class FormatContext>
  auto format(const vast::list_type& value, FormatContext& ctx)
    -> decltype(ctx.out()) {
    return format_to(ctx.out(), "list<{}>", value.value_type());
  }

  template <class FormatContext>
  auto format(const vast::map_type& value, FormatContext& ctx)
    -> decltype(ctx.out()) {
    return format_to(ctx.out(), "map<{}, {}>", value.key_type(),
                     value.value_type());
  }

  template <class FormatContext>
  auto format(const vast::record_type& value, FormatContext& ctx)
    -> decltype(ctx.out()) {
    auto out = ctx.out();
    out = format_to(out, "record {{");
    for (bool first = true; auto field : value.fields()) {
      if (first) {
        out = format_to(out, "{}", field);
        first = false;
      } else {
        out = format_to(out, ", {}", field);
      }
    }
    return format_to(out, "}}");
  }
};

template <>
struct formatter<struct vast::enumeration_type::field_view> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) -> decltype(ctx.begin()) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const struct vast::enumeration_type::field_view& value,
              FormatContext& ctx) -> decltype(ctx.out()) {
    return format_to(ctx.out(), "{}: {}", value.name, value.key);
  }
};

template <>
struct formatter<struct vast::record_type::field> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) -> decltype(ctx.begin()) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const struct vast::record_type::field& value, FormatContext& ctx)
    -> decltype(ctx.out()) {
    return format_to(ctx.out(), "{}: {}", value.name, value.type);
  }
};

template <>
struct formatter<vast::record_type::field_view> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) -> decltype(ctx.begin()) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const vast::record_type::field_view& value, FormatContext& ctx)
    -> decltype(ctx.out()) {
    return format_to(ctx.out(), "{}: {}", value.name, value.type);
  }
};

} // namespace fmt
