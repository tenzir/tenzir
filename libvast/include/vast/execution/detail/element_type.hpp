//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include <caf/detail/type_list.hpp>

namespace vast::execution::detail {

/// The *element type* defines how *logical operators* can be combined.
///
/// @note This code is written with extensibility in mind. To define an
/// additional element type, add it to this list of supported element types and
/// specialize the @ref element_type_traits template for it. The type must
/// fulfill the @ref element_type concept.
using supported_element_types = caf::detail::type_list<void, table_slice>;

template <class T>
struct element_type_traits;

template <class T>
struct element_type_traits_base {
  using type = T;
  static constexpr auto id
    = caf::detail::tl_index_of<supported_element_types, T>::value;
};

template <>
struct element_type_traits<void> final : element_type_traits_base<void> {
  static constexpr auto name = "Void";
  static constexpr auto requires_schema = false;
};

template <>
struct element_type_traits<table_slice> final
  : element_type_traits_base<table_slice> {
  static constexpr auto name = "Arrow";
  static constexpr auto requires_schema = true;
};

template <class T>
concept element_type
  = requires {
      requires std::same_as<typename element_type_traits<T>::type, T>;
      requires element_type_traits<T>::id
                 == caf::detail::tl_index_of<supported_element_types, T>::value;
      { element_type_traits<T>::name } -> std::convertible_to<std::string_view>;
      { element_type_traits<T>::requires_schema } -> std::convertible_to<bool>;
    };

/// A runtime version of @ref element_type_traits for use in type-erased code.
struct runtime_element_type {
  template <class T>
  explicit(false) constexpr runtime_element_type(element_type_traits<T>) noexcept
    : id{element_type_traits<T>::id},
      name{element_type_traits<T>::name},
      requires_schema{element_type_traits<T>::requires_schema} {
    // nop
  }

  /// The unique id of this element type. It exactly matches the index of the
  /// element type in the @ref supported_element_types list.
  const int id = {};

  /// The human-readable name of this element type.
  const std::string_view name = {};

  /// Defines whether this element type requires an input schema.
  const bool requires_schema = {};
};

} // namespace vast::execution::detail
