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

#include "vast/chunk.hpp"
#include "vast/detail/assert.hpp"
#include "vast/fwd.hpp"

#include <flatbuffers/flatbuffers.h>

namespace vast::fbs {

/// Semi-regular base class for types that wrap a FlatBuffers table.
/// @tparam Derived The type that wraps the FlatBuffers table for CRTP.
/// @tparam Root The generated FlatBuffers table root type.
template <class Derived, class Root>
class table {
public:
  // -- types and constants ----------------------------------------------------

  using derived_type = Derived;
  using root_type = Root;

  // -- constructors, destructors, and assignment operators --------------------

  /// Default-constructs an invalid FlatBuffers table. This
  /// @post `!*this`
  table() noexcept = default;

  /// Constructs and verifies a FlatBuffers table from a chunk.
  /// @note Constructs an invalid table if the chunk fails verification.
  explicit table(chunk_ptr&& chunk) noexcept {
    if (chunk) {
      auto verifier = flatbuffers::Verifier{
        reinterpret_cast<const uint8_t*>(chunk->data()), chunk->size()};
      if (verifier.template VerifyBuffer<root_type>())
        chunk_ = std::move(chunk);
    }
  }

  /// Default copy-construction and copy-assignment.
  table(const table& other) = default;
  table& operator=(const table& rhs) = default;

  /// Default move-construction and move-asignment.
  table(table&& other) noexcept = default;
  table& operator=(table&& rhs) noexcept = default;

  /// Default destructor.
  virtual ~table() noexcept = default;

  // -- properties -------------------------------------------------------------

  /// Access the underlying FlatBuffers root table.
  /// @pre `*this`
  const root_type& operator*() const noexcept {
    return *root();
  }

  /// Access the underlying FlatBuffers root table.
  /// @pre `*this`
  const root_type* operator->() const noexcept {
    return root();
  }

  /// Check whether the FlatBuffers table is valid.
  explicit operator bool() const noexcept {
    return chunk_ != nullptr;
  }

  /// Return a view on the underlying byte buffer.
  /// @pre `x`
  friend span<const byte> as_bytes(const table& x) noexcept {
    VAST_ASSERT(x);
    return as_bytes(x.chunk_);
  }

  /// Returns the size of the underlying chunk.
  size_t size() const noexcept {
    return *this ? chunk_->size() : 0;
  }

protected:
  // -- opt-in properties ------------------------------------------------------

  /// Queries whether there is exactly once reference.
  bool unique() const noexcept {
    return chunk_->unique();
  }

  /// Adds an additional step for deleting this table.
  /// @param f Function object that gets called after all previous deletion
  /// steps ran.
  template <class F>
  void add_deletion_step(F&& f) const noexcept {
    chunk_->add_deletion_step(std::forward<F>(f));
  }

  /// Access the underlying chunk.
  const chunk_ptr& chunk() const noexcept {
    return chunk_;
  }

private:
  // -- implementation details -------------------------------------------------

  /// Access the underlying FlatBuffers root table.
  /// @pre `*this`
  /// @post `result != nullptr`
  const root_type* root() const noexcept {
    VAST_ASSERT(*this);
    auto result = flatbuffers::GetRoot<Root>(chunk_->data());
    VAST_ASSERT(result);
    return result;
  }

  /// The underlying chunk.
  chunk_ptr chunk_ = nullptr;
};

} // namespace vast::fbs
