//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/table_slice.hpp"
#include "vast/type.hpp"

#include <caf/detail/type_list.hpp>

namespace vast {

/// The element type describes how logical operators in a pipeline can be
/// connected. Every logical operator has an input and an output element type.
///
/// A pipeline is an ordered list of logical operators where the output element
/// type of an operator always matches the input element type of the next
/// operator.
///
/// Every element type has a corresponding batch type, which is the unit that is
/// transferred between operators.
///
/// The following element types are currently supported:
/// - *void*:   Denotes the start of end of a pipeline, and cannot be connected
///             over. Its batch type is *std::monostate*.
/// - *chunk_ptr*:  Denotes a stream of opaque bytes. Its batch type is
/// *chunk_ptr*.
/// - *table_slice*: Denotes a stream of records. Its batch type is
/// *table_slice*.
///
/// To register a new element type *T*, add it to the *element_types* list and
/// specialize both *element_type_traits<T>* and *batch_traits<U>* for its
/// corresponding batch type *U*.

/// The list of supported element types.
using element_types = caf::detail::type_list<void, chunk_ptr, table_slice>;

/// A concept that matches all registered element types.
template <class T>
concept element_type = caf::detail::tl_contains<element_types, T>::value;

template <element_type ElementType>
struct element_type_traits;

template <>
struct element_type_traits<void> {
  static constexpr std::string_view name = "void";

  using batch = std::monostate;
};

template <>
struct element_type_traits<chunk_ptr> {
  static constexpr std::string_view name = "bytes";
  using batch = chunk_ptr;
};

template <>
struct element_type_traits<table_slice> {
  static constexpr std::string_view name = "events";
  using batch = table_slice;
};

template <element_type ElementType>
constexpr int element_type_id
  = caf::detail::tl_index_of<element_types, ElementType>::value;

/// A type-erased element type.
struct runtime_element_type {
  template <element_type ElementType>
  constexpr explicit(false)
    runtime_element_type(element_type_traits<ElementType>)
    : id{element_type_id<ElementType>},
      name{element_type_traits<ElementType>::name} {
  }

  /// The unique identifier of the element type, corresponding to
  /// *element_type_id<T>* for the element type *T*.
  const int id;

  /// The name of the element type for use in logs.
  const std::string_view name;

  friend auto
  operator<=>(runtime_element_type lhs, runtime_element_type rhs) noexcept {
    return lhs.id <=> rhs.id;
  }

  friend bool
  operator==(runtime_element_type lhs, runtime_element_type rhs) noexcept {
    return lhs.id == rhs.id;
  }
};

template <element_type T>
struct element_type_to_batch {
  using type = typename element_type_traits<T>::batch;
};

template <element_type T>
using element_type_to_batch_t = typename element_type_to_batch<T>::type;

template <class T>
struct batch_traits;

template <element_type T>
struct batch_traits<T> : batch_traits<element_type_to_batch_t<T>> {};

template <>
struct batch_traits<std::monostate> {
  static auto size(std::monostate) -> size_t {
    return {};
  }

  static auto bytes(std::monostate) -> size_t {
    return {};
  }

  static auto schema(std::monostate) -> type {
    return {};
  }
};

template <>
struct batch_traits<chunk_ptr> {
  static auto size(const chunk_ptr& bytes) -> size_t {
    return bytes ? bytes->size() : 0u;
  }

  static auto bytes(const chunk_ptr& bytes) -> size_t {
    return size(bytes);
  }

  static auto schema(const chunk_ptr&) -> type {
    return {};
  }
};

template <>
struct batch_traits<table_slice> {
  static auto size(const table_slice& events) -> size_t {
    return events.rows();
  }

  static auto bytes(const table_slice&) -> size_t {
    // FIXME: implement
    return {};
  }

  static auto schema(const table_slice& events) -> type {
    return events.schema();
  }
};

/// A type-erased batch.
using runtime_batch = caf::detail::tl_apply_t<
  caf::detail::tl_map_t<element_types, element_type_to_batch>, std::variant>;

[[nodiscard]] inline auto size(const runtime_batch& batch) noexcept -> size_t {
  return std::visit(
    []<class Batch>(const Batch& batch) noexcept {
      return batch_traits<Batch>::size(batch);
    },
    batch);
}

[[nodiscard]] inline auto bytes(const runtime_batch& batch) noexcept -> size_t {
  return std::visit(
    []<class Batch>(const Batch& batch) noexcept {
      return batch_traits<Batch>::bytes(batch);
    },
    batch);
}

[[nodiscard]] inline auto schema(const runtime_batch& batch) noexcept -> type {
  return std::visit(
    []<class Batch>(const Batch& batch) noexcept {
      return batch_traits<Batch>::schema(batch);
    },
    batch);
}

} // namespace vast
