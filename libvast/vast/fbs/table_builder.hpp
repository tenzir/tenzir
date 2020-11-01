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

#include "vast/detail/narrow.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/fwd.hpp"

#include <flatbuffers/flatbuffers.h>

#include <type_traits>

namespace vast::fbs {

/// Movable builder for types that wrap a FlatBuffers table.
/// @tparam Table The wrapped FlatBuffers table to build.
/// @tparam FileIdentifier A pointer to the function that returns the
/// FlatBuffers table's file identifier.
template <class Table, const char* (*FileIdentifier)()>
class table_builder {
public:
  // -- types and constants ----------------------------------------------------

  using table_type = Table;
  using derived_type = typename table_type::derived_type;
  using root_type = typename table_type::root_type;
  using offset_type = typename flatbuffers::Offset<root_type>;

  /// The default initial huffer size.
  inline static constexpr auto default_initial_size = size_t{1024};

  // -- sanity checks ----------------------------------------------------------

  static_assert(
    std::conjunction_v<
      std::is_base_of<table<derived_type, root_type>, table_type>,
      std::negation<std::is_same<table<derived_type, root_type>, table_type>>>,
    "Table must be derived from fbs::table");

  // -- constructors, destructors, and assigmnent operators --------------------

  /// Construct a builder with an initial buffer size.
  /// @param initial_size The initial buffer size in Bytes.
  explicit table_builder(size_t initial_size = default_initial_size) noexcept
    : builder_{initial_size} {
    // nop
  }

  /// Forbid copy-construction and copy-assignment.
  table_builder(const table_builder& other) noexcept = delete;
  table_builder& operator=(const table_builder& rhs) noexcept = delete;

  /// Default move-construction and move-assignment.
  table_builder(table_builder&& other) noexcept = default;
  table_builder& operator=(table_builder&& rhs) noexcept = default;

  /// Default destructor.
  virtual ~table_builder() noexcept = default;

  // -- properties -------------------------------------------------------------

  /// Resets the state of the builder.
  void reset() {
    do_reset();
    builder_.Reset();
  }

  /// Creates the derived FlatBuffers table type from the accumulated, internal
  /// builder state. The wrapper type must be constructible from `chunk_ptr&&`
  /// and further, supplied arguments.
  template <class... Args>
  derived_type finish(Args&&... args) {
    static_assert(std::is_constructible_v<derived_type, chunk_ptr&&, Args...>,
                  "Table must be constructible from <chunk_ptr&&, Args...>");
    builder_.Finish(create(), FileIdentifier());
    auto chunk = fbs::release(builder_);
    reset();
    return derived_type{std::move(chunk), std::forward<Args>(args)...};
  }

  /// Returns the size of the accumulated builder state in Bytes.
  size_t num_bytes() const noexcept {
    return detail::narrow_cast<size_t>(builder_.GetSize());
  }

protected:
  // -- interface --------------------------------------------------------------

  /// Resets the state of the builder implementation.
  virtual void do_reset() {
    // nop
  }

  /// Serializes data to the builder.
  /// @returns The offset to the serialized table.
  /// @note To serialize data, you typically call one of the `Create*()`
  /// functions in the generated code. Do this in depth-first order to build up
  /// a tree to the root.
  virtual offset_type create() = 0;

  /// The underlying FlatBuffers builder.
  flatbuffers::FlatBufferBuilder builder_;
};

} // namespace vast::fbs
