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

#include <caf/detail/apply_args.hpp>
#include <caf/detail/int_list.hpp>
#include <caf/detail/type_list.hpp>
#include <caf/meta/omittable_if_none.hpp>
#include <caf/meta/type_name.hpp>
#include <caf/sum_type_access.hpp>
#include <caf/sum_type_token.hpp>
#include <fmt/core.h>

#include <compare>

namespace vast {

/// The list of concrete types.
using concrete_types = caf::detail::type_list<none_type, bool_type>;

/// A concept that models any concrete type.
template <class T>
concept concrete_type = requires(const T& type) {
  requires caf::detail::tl_contains<concrete_types, T>::value;
  { T::type_index() } -> concepts::same_as<uint8_t>;
  { as_bytes(type) } -> concepts::same_as<std::span<const std::byte>>;
};

/// A concept that models basic concrete types, i.e., types that do not hold
/// additional state.
template <class T>
concept basic_type = requires(const T& type) {
  requires concrete_type<T>;
  requires std::is_trivial_v<T>;
};

// TODO: Add a concept for complex types.

// -- type --------------------------------------------------------------------

/// The sematic representation of data.
class type final {
public:
  /// Indiciates whether to skip over alias and tag types when looking at the
  /// underlying FlatBuffers representation.
  enum class transparent {
    yes, ///< Skip alias and tag types.
    no,  ///< Include alias and tag types.
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
  /// @note The chunk may extend past the end of the actual table, although it
  /// is generally recommended not to do so because that increases the payload
  /// size when sending types in messages over the wire via CAF. Please make
  /// sure to specify both start and end when slicing chunks for nested
  /// `vast.fbs.Type` FlatBuffers tables.
  /// @note The table offsets are verified only when assertions are enabled.
  /// @pre `table != nullptr`
  explicit type(chunk_ptr table) noexcept;

  /// Implicitly construct a type from a basic concrete type.
  template <basic_type T>
  type(const T& other) noexcept
    : type{chunk::make(as_bytes(other), []() noexcept {})} {
    // nop
  }

  // TODO: Add an implicit constructor for complex types.

  /// Constructs a named type.
  /// @param name The type name.
  /// @param nested The aliased type.
  /// @note Creates a copy of nested if the provided name is empty.
  type(std::string_view name, const type& nested) noexcept;

  /// Constructs a type from a legacy_type.
  /// @relates legacy_type
  explicit type(const legacy_type& other) noexcept;

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
    return f(caf::meta::type_name("vast.type"), caf::meta::omittable_if_none(),
             x.table_);
  }

  /// Returns the name of this type.
  [[nodiscard]] std::string_view name() const& noexcept;
  [[nodiscard]] std::string_view name() && = delete;

private:
  /// The underlying representation of the type.
  chunk_ptr table_ = {};
};

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

  template <vast::basic_type T, int Index>
  static const T& get(const vast::type&, sum_type_token<T, Index>) {
    static const auto instance = T{};
    return instance;
  }

  // TODO: Implement get() for complex types.

  template <vast::concrete_type T, int Index>
  static const T* get_if(const vast::type* x, sum_type_token<T, Index> token) {
    if (x && is(*x, token))
      return &get(*x, token);
    return nullptr;
  }

  template <class Result, class Visitor, class... Ts>
  static Result apply(const vast::type& x, Visitor&& v, Ts&&... xs) {
    auto xs_as_tuple = std::forward_as_tuple(xs...);
    auto indices = caf::detail::get_indices(xs_as_tuple);
    return caf::detail::apply_args_suffxied(v, std::move(indices),
                                            std::move(xs_as_tuple), x);
  }
};

} // namespace caf

// -- formatter ---------------------------------------------------------------

namespace fmt {

template <>
struct formatter<vast::type> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) -> decltype(ctx.begin()) {
    // TODO: Support format specifiers to format more than just names.
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const vast::type& value, FormatContext& ctx)
    -> decltype(ctx.out()) {
    return format_to(ctx.out(), "{}", value.name());
  }
};

template <vast::concrete_type T>
struct formatter<T> : formatter<vast::type> {};

} // namespace fmt
