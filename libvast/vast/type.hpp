//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/chunk.hpp"
#include "vast/concepts.hpp"
#include "vast/detail/range.hpp"
#include "vast/detail/stack_vector.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/offset.hpp"

#include <caf/detail/apply_args.hpp>
#include <caf/detail/int_list.hpp>
#include <caf/detail/type_list.hpp>
#include <caf/meta/omittable_if_none.hpp>
#include <caf/meta/type_name.hpp>
#include <caf/sum_type.hpp>
#include <fmt/format.h>

#include <compare>
#include <initializer_list>

namespace vast {

/// The list of concrete types.
using concrete_types
  = caf::detail::type_list<none_type, bool_type, integer_type, count_type,
                           real_type, duration_type, time_type, string_type,
                           pattern_type, address_type, subnet_type,
                           enumeration_type, list_type, map_type, record_type>;

/// A concept that models any concrete type.
template <class T>
concept concrete_type = requires(const T& value) {
  requires caf::detail::tl_contains<concrete_types, T>::value;
  requires std::is_final_v<T>;
  { T::type_index() } -> concepts::same_as<uint8_t>;
  { as_bytes(value) } -> concepts::same_as<std::span<const std::byte>>;
};

/// A concept that models any concrete type, or the abstract type class itself.
template <class T>
concept type_or_concrete_type = std::is_same_v<T, type> || concrete_type<T>;

/// A concept that models basic concrete types, i.e., types that do not hold
/// additional state.
template <class T>
concept basic_type = requires {
  requires concrete_type<T>;
  requires std::is_empty_v<T>;
  requires std::is_trivial_v<T>;
};

/// A concept that models basic concrete types, i.e., types that hold
/// additional state and extend the lifetime of the surrounding type.
template <class T>
concept complex_type = requires(const T& value) {
  requires concrete_type<T>;
  requires std::is_base_of_v<type, T>;
  requires sizeof(T) == sizeof(chunk_ptr);
};

// -- type --------------------------------------------------------------------

/// The sematic representation of data.
class type {
public:
  /// An owned key-value type annotation.
  struct tag final {
    std::string key;                       ///< The key.
    std::optional<std::string> value = {}; ///< The value (optional).
  };

  /// A view on a key-value type annotation.
  struct tag_view final {
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

  /// Implicitly construct a type from a basic concrete type.
  template <basic_type T>
  type(const T& other) noexcept
    : type{chunk::make(as_bytes(other), []() noexcept {})} {
    // nop
  }

  /// Implicitly construct a type from a complex concrete type.
  template <complex_type T>
  type(const T& other) noexcept : table_{other.table_} {
    // nop
  }

  /// Implicitly construct a type from a complex concrete type.
  template <complex_type T>
    requires(std::is_rvalue_reference_v<T>)
  type(T&& other) // NOLINT(bugprone-forwarding-reference-overload)
  noexcept : table_{std::move(other.table_)} {
    // nop
  }

  /// Constructs a named and tagged type.
  /// @param name The type name.
  /// @param nested The aliased type.
  /// @param tags The key-value type annotations.
  /// @note Creates a copy of nested if the provided name and tags are empty.
  type(std::string_view name, const type& nested,
       const std::vector<struct tag>& tags) noexcept;

  /// Constructs a named type.
  /// @param name The type name.
  /// @param nested The aliased type.
  /// @note Creates a copy of nested if the provided name is empty.
  type(std::string_view name, const type& nested) noexcept;

  /// Constructs a tagged type.
  /// @param nested The aliased type.
  /// @param tags The key-value type annotations.
  /// @note Creates a copy of nested if the tags are empty.
  type(const type& nested, const std::vector<struct tag>& tags) noexcept;

  /// Constructs a type from a legacy_type.
  /// @relates legacy_type
  explicit type(const legacy_type& other) noexcept;

  /// Converts a type into a legacy_type.
  /// @note The roundtrip `type{legacy_type{*this}}` will produce a different
  /// type beccause of the inconsistent handling of names for legacy types. The
  /// types will be semantically equivalent, but may not compare equal.
  /// @relates legacy_type
  explicit operator legacy_type() const noexcept;

  /// Returns whether the type contains a conrete type other than the *none_type*.
  [[nodiscard]] explicit operator bool() const noexcept;

  /// Compares the underlying representation of two types for equality.
  friend bool operator==(const type& lhs, const type& rhs) noexcept;

  /// Compares the underlying representation of two types lexicographically.
  friend std::strong_ordering
  operator<=>(const type& lhs, const type& rhs) noexcept;

  /// Returns the underlying FlatBuffers table representation.
  /// @note Include `vast/fbs/type.hpp` to be able to use this function.
  /// @param transparent Whether to skip over alias and tag types
  [[nodiscard]] const fbs::Type&
  table(enum transparent transparent) const noexcept;

  /// Returns the concrete type index of this type.
  [[nodiscard]] uint8_t type_index() const noexcept;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const type& x) noexcept;

  /// Enables integration with CAF's type inspection.
  template <class Inspector>
  friend typename Inspector::result_type inspect(Inspector& f, type& x) {
    return f(caf::meta::type_name("vast.type"), x.table_);
  }

  /// Returns the name of this type.
  /// @note The result is empty if the contained type is unnammed. Built-in
  /// types have no name. Use the {fmt} API to render a type's signature.
  [[nodiscard]] std::string_view name() const& noexcept;
  [[nodiscard]] std::string_view name() && = delete;

  /// Returns the value of a tag by name, if it exists.
  /// @param key The key of the tag.
  /// @note If a tag exists and its value is empty, the result contains an empty
  /// string to. If the tag does not exists, the result is `std::nullopt`.
  // TODO: The generated FlatBuffers code does not work with `std::string_view`.
  // Re-evaluate when upgrading to FlatBuffers 2.0
  [[nodiscard]] std::optional<std::string_view>
  tag(const char* key) const& noexcept;
  [[nodiscard]] std::optional<std::string_view>
  tag(const char* key) && = delete;

  /// Returns a view on all tags.
  [[nodiscard]] std::vector<tag_view> tags() const& noexcept;
  [[nodiscard]] std::vector<tag_view> tags() && = delete;

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

protected:
  /// The underlying representation of the type.
  chunk_ptr table_ = {}; // NOLINT
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

// -- none_type ---------------------------------------------------------------

/// Represents a default constructed type.
/// @relates type
class none_type final {
public:
  /// Returns the type index.
  static uint8_t type_index() noexcept;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const none_type&) noexcept;
};

// -- bool_type ---------------------------------------------------------------

/// A boolean value that can either be true or false.
/// @relates type
class bool_type final {
public:
  /// Returns the type index.
  static uint8_t type_index() noexcept;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const bool_type&) noexcept;
};

// -- integer_type ------------------------------------------------------------

/// A signed integer.
/// @relates type
class integer_type final {
public:
  /// Returns the type index.
  static uint8_t type_index() noexcept;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const integer_type&) noexcept;
};

// -- count_type --------------------------------------------------------------

/// An unsigned integer.
/// @relates type
class count_type final {
public:
  /// Returns the type index.
  static uint8_t type_index() noexcept;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const count_type&) noexcept;
};

// -- real_type ---------------------------------------------------------------

/// A floating-point value.
/// @relates type
class real_type final {
public:
  /// Returns the type index.
  static uint8_t type_index() noexcept;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const real_type&) noexcept;
};

// -- duration_type -----------------------------------------------------------

/// A time interval.
/// @relates type
class duration_type final {
public:
  /// Returns the type index.
  static uint8_t type_index() noexcept;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const duration_type&) noexcept;
};

// -- time_type ---------------------------------------------------------------

/// A point in time.
/// @relates type
class time_type final {
public:
  /// Returns the type index.
  static uint8_t type_index() noexcept;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const time_type&) noexcept;
};

// -- string_type --------------------------------------------------------------

/// A string of characters.
/// @relates type
class string_type final {
public:
  /// Returns the type index.
  static uint8_t type_index() noexcept;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const string_type&) noexcept;
};

// -- pattern_type ------------------------------------------------------------

/// A regular expression.
/// @relates type
class pattern_type final {
public:
  /// Returns the type index.
  static uint8_t type_index() noexcept;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const pattern_type&) noexcept;
};

// -- address_type ------------------------------------------------------------

/// An IP address (v4 or v6).
/// @relates type
class address_type final {
public:
  /// Returns the type index.
  static uint8_t type_index() noexcept;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const address_type&) noexcept;
};

// -- subnet_type -------------------------------------------------------------

/// A CIDR subnet.
/// @relates type
class subnet_type final {
public:
  /// Returns the type index.
  static uint8_t type_index() noexcept;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const subnet_type&) noexcept;
};

// -- enumeration_type --------------------------------------------------------

/// An enumeration type that can have one specific value.
/// @relates type
class enumeration_type final : private type {
  friend class type;
  friend struct caf::sum_type_access<vast::type>;

public:
  /// A field of an enumeration type.
  struct field final {
    std::string name;                 ///< The mame of the field.
    std::optional<uint32_t> key = {}; ///< The optional index of the field.
  };

  /// A view on a field of an enumeration type.
  struct field_view final {
    std::string_view name; ///< The mame of the field.
    uint32_t key;          /// The index of the field.
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

  /// Constructs an enumeration type.
  /// @pre `!fields.empty()`
  explicit enumeration_type(const std::vector<struct field>& fields) noexcept;

  /// Returns the type index.
  static uint8_t type_index() noexcept;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte>
  as_bytes(const enumeration_type& x) noexcept;

  /// Returns the field at the given key, or an empty string if it does not exist.
  [[nodiscard]] std::string_view field(uint32_t key) const& noexcept;
  [[nodiscard]] std::string_view field(uint32_t key) && = delete;

  /// Returns a view onto all fields, sorted by key.
  [[nodiscard]] std::vector<field_view> fields() const& noexcept;
  [[nodiscard]] std::vector<field_view> fields() && = delete;
};

// -- list_type ---------------------------------------------------------------

/// An ordered sequence of values.
/// @relates type
class list_type final : private type {
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

  /// Returns the type index.
  static uint8_t type_index() noexcept;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const list_type& x) noexcept;

  /// Returns the nested value type.
  [[nodiscard]] type value_type() const noexcept;
};

// -- map_type ----------------------------------------------------------------

/// An associative mapping from keys to values.
/// @relates type
class map_type final : private type {
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

  /// Returns the type index.
  static uint8_t type_index() noexcept;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const map_type& x) noexcept;

  /// Returns the nested key type.
  [[nodiscard]] type key_type() const noexcept;

  /// Returns the nested value type.
  [[nodiscard]] type value_type() const noexcept;
};

// -- record_type -------------------------------------------------------------

/// A list of fields, each of which have a name and type.
/// @relates type
class record_type final : private type {
  friend class type;
  friend struct caf::sum_type_access<vast::type>;

  /// An iterable view over the fields of a record type.
  struct iterable;

  /// An iterable view over the leaf fields of a record type.
  struct leaf_iterable;

public:
  /// A record type field.
  struct field final {
    std::string name; ///< The mame of the field.
    class type type;  ///< The type of the field.
  };

  /// A sliced view on a record type field.
  struct field_view final {
    std::string_view name; /// The name of the field.
    class type type;       /// The type of the field.
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

  /// Constructs a record type from a set of field views.
  /// @param fields The ordered fields of the record type.
  /// @pre `!fields.empty()`
  explicit record_type(const std::vector<struct field_view>& fields) noexcept;
  explicit record_type(std::initializer_list<struct field_view> fields) noexcept;

  /// Constructs a record type from a set of fields.
  /// @param fields The ordered fields of the record type.
  /// @pre `!fields.empty()`
  explicit record_type(const std::vector<struct field>& fields) noexcept;

  /// Returns the type index.
  static uint8_t type_index() noexcept;

  /// Returns a view of the underlying binary representation.
  friend std::span<const std::byte> as_bytes(const record_type& x) noexcept;

  /// Returns an iterable view over the fields of a record type.
  [[nodiscard]] iterable fields() const noexcept;

  /// Returns an iterable view over the leaf fields of a record type.
  [[nodiscard]] leaf_iterable leaves() const noexcept;

  /// Returns the field at the given index.
  [[nodiscard]] field_view field(size_t index) const noexcept;

  /// Returns the field at the given offset.
  [[nodiscard]] field_view field(offset index) const noexcept;

  /// Returns a new, flattened record type.
  friend record_type flatten(const record_type& type) noexcept;
};

/// An iterable over the fields of a record.
struct record_type::iterable final : detail::range_facade<struct iterable> {
  friend class record_type;
  friend class detail::range_facade<struct iterable>;

  /// Access a field by index.
  [[nodiscard]] field_view operator[](size_t index) const noexcept;

  /// Get the number of fields in the record field.
  [[nodiscard]] size_t size() const noexcept;

private:
  /// Constructs an iterable from a record type.
  explicit iterable(record_type type) noexcept;

  // Range facade implementation details.
  void next() noexcept;
  [[nodiscard]] bool done() const noexcept;
  [[nodiscard]] field_view get() const noexcept;

  size_t index_;     ///< The index of the currently selected field.
  record_type type_; ///< The record type we're iterating over.
};

/// An iterable over the leaf fields of a record.
struct record_type::leaf_iterable final
  : detail::range_facade<struct leaf_iterable> {
  friend class record_type;
  friend class detail::range_facade<struct leaf_iterable>;

private:
  /// Constructs a leaf_iterable from a record type.
  explicit leaf_iterable(record_type type) noexcept;

  // Range facade implementation details.
  void next() noexcept;
  [[nodiscard]] bool done() const noexcept;
  [[nodiscard]] std::pair<field_view, offset> get() const noexcept;

  offset index_;     ///< The offset of the currently selected leaf field.
  record_type type_; ///< The record type we're iterating over.
};

} // namespace vast

// -- sum_type_access ---------------------------------------------------------

namespace caf {

template <>
struct sum_type_access<vast::type> final {
  using types = vast::concrete_types;
  using type0 = vast::none_type;
  static constexpr bool specialized = true;

  template <vast::concrete_type T, int Index>
  static bool is(const vast::type& x, sum_type_token<T, Index>) {
    return x.type_index() == T::type_index();
  }

  template <class T, int Index>
  static bool is(const vast::type&, sum_type_token<T, Index>) {
    static_assert(vast::detail::always_false_v<T>, "T must be a concrete type");
  }

  template <vast::basic_type T, int Index>
  static const T& get(const vast::type&, sum_type_token<T, Index>) {
    static const auto instance = T{};
    return instance;
  }

  template <vast::complex_type T, int Index>
  static const T& get(const vast::type& x, sum_type_token<T, Index>) {
    return static_cast<const T&>(x);
  }

  template <class T, int Index>
  static const T& get(const vast::type&, sum_type_token<T, Index>) {
    static_assert(vast::detail::always_false_v<T>, "T must be a concrete type");
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
  }

  template <vast::concrete_type... Ts, uint8_t... Indices>
  static uint8_t index_from_type(caf::detail::type_list<Ts...>,
                                 std::integer_sequence<uint8_t, Indices...>,
                                 const vast::type& x) noexcept {
    static const auto table = []() noexcept {
      std::array<uint8_t, std::numeric_limits<uint8_t>::max()> tbl{};
      tbl.fill(std::numeric_limits<uint8_t>::max());
      (static_cast<void>(tbl[Ts::type_index()] = Indices), ...);
      return tbl;
    }();
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
    auto result = table[x.type_index()];
    VAST_ASSERT(result != std::numeric_limits<uint8_t>::max());
    return result;
  }

  template <class Result, class Visitor, vast::concrete_type... Ts,
            uint8_t... Indices, class... Args>
  static auto dispatch(caf::detail::type_list<Ts...> tl,
                       std::integer_sequence<uint8_t, Indices...> is,
                       const vast::type& x, Visitor&& v, Args&&... xs)
    -> Result {
    static constexpr auto table = std::array{
      +[](const vast::type& x, Visitor&& v, Args&&... xs) -> Result {
        auto xs_as_tuple = std::forward_as_tuple(xs...);
        auto indices = caf::detail::get_indices(xs_as_tuple);
        return caf::detail::apply_args_suffxied(
          std::forward<decltype(v)>(v), std::move(indices), xs_as_tuple,
          get(x, sum_type_token<Ts, Indices>{}));
      }...};
    const auto dispatch = table[index_from_type(tl, is, x)];
    VAST_ASSERT(dispatch);
    return dispatch(x, std::forward<Visitor>(v), std::forward<Args>(xs)...);
  }

  template <class Result, class Visitor, class... Args>
  static auto apply(const vast::type& x, Visitor&& v, Args&&... xs) -> Result {
    return dispatch<Result>(
      types{},
      std::make_integer_sequence<uint8_t, caf::detail::tl_size<types>::value>{},
      x, std::forward<Visitor>(v), std::forward<Args>(xs)...);
  }
};

} // namespace caf

// -- standard library specializations ----------------------------------------

namespace std {

/// Byte-wise hashing for types.
/// @note The implementation is from Boost.hash_combine.
template <vast::type_or_concrete_type T>
struct hash<T> {
  auto operator()(const T& type) const noexcept {
    const auto bytes = as_bytes(type);
    auto seed = bytes.size();
    for (const auto i : bytes)
      seed ^= static_cast<uint8_t>(i) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    return seed;
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
    if (const auto& tags = value.tags(); !tags.empty())
      out = format_to(out, " {}", fmt::join(tags, " "));
    return out;
  }
};

template <>
struct formatter<vast::type::tag_view> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) -> decltype(ctx.begin()) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const vast::type::tag_view& value, FormatContext& ctx)
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
    return format_to(ctx.out(), "integer");
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
    return format_to(ctx.out(), "address");
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
    return format_to(ctx.out(), "record {{{}}}",
                     fmt::join(value.fields(), ", "));
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
