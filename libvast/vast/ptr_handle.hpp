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

#include <cstddef>
#include <type_traits>

#include <caf/intrusive_ptr.hpp>

namespace vast {

/// Wraps a `caf::intrusive_ptr<T>` to give it a distinct type.
template <class T>
class ptr_handle {
public:
  // -- member types -----------------------------------------------------------

  /// Pointer to a `T`.
  using pointer = std::add_pointer_t<T>;

  /// Pointer to an immutable `T`.
  using const_pointer = std::add_const_t<pointer>;

  /// Reference to a `T`.
  using reference = std::add_lvalue_reference_t<T>;

  /// Reference to an immutable `T`.
  using const_reference = std::add_const_t<reference>;

  // -- constructors, destructors, and assignment operators --------------------

  ptr_handle() = default;

  ptr_handle(ptr_handle&&) = default;

  ptr_handle(const ptr_handle&) = default;

  ptr_handle& operator=(ptr_handle&&) = default;

  ptr_handle& operator=(const ptr_handle&) = default;

  explicit ptr_handle(caf::intrusive_ptr<T> ptr) : ptr_(std::move(ptr)) {
    // nop
  }

  virtual ~ptr_handle() {
    // nop
  }

  // -- properties -------------------------------------------------------------

  /// @returns the stored pointer.
  pointer get() noexcept {
    return ptr_.get();
  }

  /// @returns the stored pointer.
  const_pointer get() const noexcept {
    return ptr_.get();
  }

  /// @returns the stored smart pointer.
  const caf::intrusive_ptr<T>& ptr() const noexcept {
    return ptr_;
  }

  // -- comparison -------------------------------------------------------------

  /// @returns `get() - other.get()` for total ordering of handles by comparing
  ///           the pointer values.
  ptrdiff_t compare(const ptr_handle& other) const noexcept {
    return get() - other.get();
  }

  // -- operators --------------------------------------------------------------

  reference operator*() {
    return *get();
  }

  const_reference operator*() const {
    return *get();
  }

  pointer operator->() {
    return get();
  }

  const_pointer operator->() const {
    return get();
  }

  bool operator!() const noexcept {
    return get() == nullptr;
  }

  explicit operator bool() const noexcept {
    return get() != nullptr;
  }

protected:
  // -- member variables -------------------------------------------------------

  caf::intrusive_ptr<T> ptr_;
};

// -- related free functions ---------------------------------------------------

/// @relates ptr_handle
template <class T>
bool operator==(const ptr_handle<T>& x, std::nullptr_t) {
  return !x;
}

/// @relates ptr_handle
template <class T>
bool operator==(std::nullptr_t, const ptr_handle<T>& x) {
  return !x;
}

/// @relates ptr_handle
template <class T>
bool operator!=(const ptr_handle<T>& x, std::nullptr_t) {
  return static_cast<bool>(x);
}

/// @relates ptr_handle
template <class T>
bool operator!=(std::nullptr_t, const ptr_handle<T>& x) {
  return static_cast<bool>(x);
}

} // namespace vast
