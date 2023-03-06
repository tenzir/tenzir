//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/fwd.hpp"

#include "vast/table_slice.hpp"
#include "vast/type.hpp"

namespace vast {

/// Element type tag types.
struct bytes;
struct events;

using element_types = caf::detail::type_list<void, bytes, events>;

template <class T>
concept element_type = caf::detail::tl_contains<element_types, T>::value;

template <element_type ElementType>
struct element_type_traits;

template <element_type ElementType>
constexpr int element_type_id
  = caf::detail::tl_index_of<element_types, ElementType>::value;

template <>
struct element_type_traits<void> {
  static constexpr std::string_view name = "void";

  using batch = std::monostate;

  static auto size(batch) -> size_t {
    return {};
  }

  static auto bytes(batch) -> size_t {
    return {};
  }

  static auto schema(batch) -> type {
    return {};
  }
};

template <>
struct element_type_traits<bytes> {
  static constexpr std::string_view name = "bytes";
  using batch = chunk_ptr;

  static auto size(const batch& bytes) -> size_t {
    return bytes ? bytes->size() : 0u;
  }

  static auto bytes(const batch& bytes) -> size_t {
    return size(bytes);
  }

  static auto schema(const batch&) -> type {
    return {};
  }
};

template <>
struct element_type_traits<events> {
  static constexpr std::string_view name = "events";
  using batch = table_slice;

  static auto size(const batch& events) -> size_t {
    return events.rows();
  }

  static auto bytes(const batch&) -> size_t {
    // FIXME: implement
    return {};
  }

  static auto schema(const batch& events) -> type {
    return events.schema();
  }
};

struct runtime_element_type {
  template <element_type ElementType>
  explicit(false) runtime_element_type(element_type_traits<ElementType>)
    : id{element_type_id<ElementType>} {
  }

  int id;

  // TODO: Do we need name?
};

} // namespace vast
