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
#include "vast/span.hpp"

#include <utility>
#include <vector>

namespace vast::detail {

/// A collection of data structures that can be used interchangeably from
/// application memory or straight from disk using a memory mapping.

// There are three dimensions involved here:
//
// builder format <-> packed format
//        mutable <-> immutable
//          owned <-> view
//
// These are not completely independent, and not all combinations are useful.
// The two that are at least required are `standalone` below for building up
// the data, and `view` for using the data inside a bigger map, e.g. in a
// partition with multiple indexers.
//
// There's a third category that might be worth implementing, which is a
// standalone region with the mmapped layout and exclusive ownership. This would
// even allow mutable access to the data, for some data structures.

// Note that the idea is that this can be used transparently for code that does
// not need to care about the storage format used, but on a high-level the
// programmer *does* need to care and use the correct specific storage type for
// the current situation.

// TODO: Rename this to something that is not `mms`, since that is already the
// name of a separate library. Candidates:
//   - flatdata
//   - zerods
//   - dds (direct data structures)
//   - mapdata
//   - storemap
//   - storagebuffers

enum class mms {
  standalone, // builder format, mutable,   owned
  flat,       // packed format,  mutable,   owned
  view,       // packed format,  immutable, view
};

template <enum mms P, class T>
class mms_vector;

// Typedefs.
template <typename T>
using vector_standalone = mms_vector<mms::standalone, T>;

template <typename T>
using vector_flat = mms_vector<mms::flat, T>;

template <typename T>
using vector_view = mms_vector<mms::view, T>;

// Standalone vector.
// Note that this is implicitly compatible with `inspect()`, since std::vector
// is already inspectable.
template <class T>
class mms_vector<mms::standalone, T> : public std::vector<T> {
public:
  using std::vector<T>::vector; // inherit constructors

  using standalone_type = mms_vector<mms::standalone, T>;
  using flat_type = mms_vector<mms::flat, T>;
  using view_type = mms_vector<mms::view, T>;

  mms_vector(span<const T>) = delete;

  standalone_type to_standalone() const = delete;
  view_type to_view() const = delete;
  flat_type to_flat() const;
};

// A mmapped vector is a non-owning region of standalone with a size, containing
// that many adjacent instances of a type T.
// Conveniently, we already have a `span` class that models exactly that.
template <class T>
class mms_vector<mms::view, T> : public span<const T> {
public:
  using standalone_type = mms_vector<mms::standalone, T>;
  using flat_type = mms_vector<mms::flat, T>;
  using view_type = mms_vector<mms::view, T>;

  mms_vector(span<const T> data) : span<const T>(data) {
  }

  // Explicitly delete the std::vector constructors so we get a nice error
  // message rather than thousands of lines of SFINAE failures on misuse.
  mms_vector(size_t, const T& = T{}) = delete;
  mms_vector(std::initializer_list<T>) = delete;
  template <typename InputIt>
  mms_vector(InputIt, InputIt) = delete;

  mms_vector<mms::flat, T> to_flat() const;
  mms_vector<mms::standalone, T> to_standalone() const;
  mms_vector<mms::flat, T> to_view() const = delete;
};

// A `flat` vector uses the same standalone layout as the `mmapped` variant, but
// owns the standalone it is pointing to.
template <class T>
class mms_vector<mms::flat, T> : public span<T> {
public:
  using standalone_type = mms_vector<mms::standalone, T>;
  using flat_type = mms_vector<mms::flat, T>;
  using view_type = mms_vector<mms::view, T>;

  mms_vector(chunk_ptr&& c)
    : span<T>(c->data(), c->size() / sizeof(T)), chunk_(c) {
  }

  // Explicitly fail the std::vector constructors so we get a nice error
  // message rather than thousands of lines of SFINAE failures on misuse.
  mms_vector(size_t, const T& = T{}) = delete;
  mms_vector(std::initializer_list<T>) = delete;
  template <typename InputIt>
  mms_vector(InputIt, InputIt) = delete;

  mms_vector<mms::standalone, T> to_standalone() const = delete;

private:
  chunk_ptr chunk_;
};

// TODO: These would probably look better as free functions.

template <class T>
auto vector_standalone<T>::to_flat() const -> flat_type {
  // Since `std::vector` is already a contiguous data structure, we just need
  // to copy the contents into some standalone we own.
  auto c = chunk::copy(static_cast<const std::vector<T>*>(this));
  return buffer_type(std::move(c));
}

template <typename T>
auto vector_view<T>::to_flat() const -> flat_type {
  return mms_vector<mms::flat, T>(this->begin(), this->end());
}

template <typename T>
auto vector_view<T>::to_standalone() const -> standalone_type {
  return mms_vector<mms::standalone, T>(this->begin(), this->end());
}

} // namespace vast::detail