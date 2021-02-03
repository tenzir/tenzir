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

#include "vast/span.hpp"

#include <utility>
#include <vector>

namespace vast::detail {

// FIXME: There are three dimensions involved here:
//
// builder format <-> packed format
//        mutable <-> immutable
//          owned <-> view
//
// These are not completely independent, and not all combinations are useful.
// The two that are at least required are `standalone` below for building up the
// data, and `memory_view` for using the data inside a bigger map, e.g. in a
// partition with multiple indexers.
//
// There's a third category that might be worth implementing, which is a
// memory region with the mmapped layout and exclusive ownership. This would
// even allow mutable access to the data, for some data structures.

enum class mms {
  standalone,  // builder format, mutable, owned
  memory_view, // packed format, immutable, view
  // memory_owned,  // packed format, mutable, owned
};

// FIXME: move elsewhere
template <typename T>
struct false_t {
  static const bool value = false;
};

template <enum mms P, class T>
class mms_vector;

template <class T>
class mms_vector<mms::standalone, T> : public std::vector<T> {
public:
  using std::vector<T>::vector; // inherit constructors

  using standalone_type = mms_vector;
  using mview_type = mms_vector<mms::memory_view, T>;

  mms_vector(span<const T>) {
    static_assert(false_t<T>::value, "cannot construct vector<T> from span");
  }

  // TODO: Find a better name for this.
  mms_vector make_standalone() const {
    // We could also just make a copy of ourselves, or even return `*this` as
    // `const&`, but until we actually have a use case this way can catch
    // accidental copies.
    static_assert(false_t<T>::value, "vector is already standalone");
  }

  template <typename Writer>
  void write_to(Writer w) {
    // [...]
  }
};

// A memory_view vector is just a non-owning region of memory with a size.
// Conveniently, we already have a `span` class that models exactly that.
template <class T>
class mms_vector<mms::memory_view, T> : public span<const T> {
public:
  mms_vector(span<const T> data) : span<const T>(data) {
  }

  // Explicitly fail the std::vector constructors so we get a nice error
  // message rather than thousands of lines of SFINAE failures on misuse.
  mms_vector(size_t, const T& = T{}) {
    static_assert(false_t<T>::value, "cannot construct static vector from "
                                     "size and default value");
  }

  mms_vector(std::initializer_list<T>) {
    static_assert(false_t<T>::value, "cannot construct static vector from "
                                     "initializer list");
  }

  template <typename InputIt>
  mms_vector(InputIt, InputIt) {
    static_assert(false_t<T>::value, "cannot construct static vector from "
                                     "iterator range");
  }

  mms_vector<mms::standalone, T> make_standalone() const {
    return mms_vector<mms::standalone, T>(this->begin(), this->end());
  }
};

} // namespace vast::detail