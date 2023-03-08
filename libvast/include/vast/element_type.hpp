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

/// Element type tag types.
struct bytes;
struct events;

using element_types = caf::detail::type_list<void, bytes, events>;

template <class T>
concept element_type = caf::detail::tl_contains<element_types, T>::value;

template <class T>
struct batch_traits;

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

template <element_type ElementType>
struct element_type_traits;

template <>
struct element_type_traits<void> {
  static constexpr std::string_view name = "void";

  using batch = std::monostate;
};

template <>
struct element_type_traits<bytes> {
  static constexpr std::string_view name = "bytes";
  using batch = chunk_ptr;
};

template <>
struct element_type_traits<events> {
  static constexpr std::string_view name = "events";
  using batch = table_slice;
};

template <element_type ElementType>
constexpr int element_type_id
  = caf::detail::tl_index_of<element_types, ElementType>::value;

struct runtime_element_type {
  template <element_type ElementType>
  explicit(false) runtime_element_type(element_type_traits<ElementType>)
    : id{element_type_id<ElementType>},
      name{element_type_traits<ElementType>::name} {
  }

  int id;
  std::string_view name;

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

struct runtime_batch
  : caf::detail::tl_apply_t<
      caf::detail::tl_map_t<element_types, element_type_to_batch>, std::variant> {
  using variant::variant;

  [[nodiscard]] auto size() const noexcept -> size_t {
    return std::visit(
      []<class Batch>(const Batch& batch) noexcept {
        return batch_traits<Batch>::size(batch);
      },
      *this);
  }

  [[nodiscard]] auto bytes() const noexcept -> size_t {
    return std::visit(
      []<class Batch>(const Batch& batch) noexcept {
        return batch_traits<Batch>::bytes(batch);
      },
      *this);
  }

  [[nodiscard]] auto schema() const noexcept -> type {
    return std::visit(
      []<class Batch>(const Batch& batch) noexcept {
        return batch_traits<Batch>::schema(batch);
      },
      *this);
  }
};

} // namespace vast
