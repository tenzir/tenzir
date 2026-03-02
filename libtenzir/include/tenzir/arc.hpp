//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/assert.hpp"

#include <memory>
#include <type_traits>
#include <utility>

#pragma once

namespace tenzir {

template <class T>
class Arc;

namespace detail {

template <class T>
constexpr bool is_arc_v = false;
template <class T>
constexpr bool is_arc_v<Arc<T>> = true;

} // namespace detail

/// A non-null `std::shared_ptr` that propagates `const`.
///
/// Copying is always cheap (refcount increment, no deep copy). The user is
/// responsible for synchronization when mutating through multiple copies.
///
/// Moving leaves the `Arc` in an invalid state that must not be used
/// afterwards. All access is checked for this invariant.
template <class T>
class Arc {
public:
  /// Constructs an arc from an existing non-null `shared_ptr`.
  static auto from_non_null(std::shared_ptr<T> ptr) -> Arc<T> {
    return Arc{std::move(ptr)};
  }

  Arc(Arc const&) = default;
  auto operator=(Arc const&) -> Arc& = default;
  Arc(Arc&&) = default;
  auto operator=(Arc&&) -> Arc& = default;
  ~Arc() = default;

  Arc() = default;

  /// Constructs an arc from a value whose `shared_ptr` is compatible with `T`.
  /// Preserves the dynamic type of `U` (no slicing for polymorphic types).
  template <class U>
    requires std::convertible_to<std::shared_ptr<U>, std::shared_ptr<T>>
  explicit(false) Arc(U x) : ptr_{std::make_shared<U>(std::move(x))} {
  }

  /// Constructs an arc by converting `U` into `T`. This is not selected for
  /// types that are pointer-convertible to prevent slicing.
  template <class U>
    requires(not std::convertible_to<std::shared_ptr<U>, std::shared_ptr<T>>)
            and std::constructible_from<T, U>
            and (not detail::is_arc_v<std::remove_cvref_t<U>>)
  explicit(false) Arc(U x) : ptr_{std::make_shared<T>(std::move(x))} {
  }

  /// Constructs an arc with in-place construction of `T`.
  template <class... Args>
  explicit Arc(std::in_place_t, Args&&... args)
    : ptr_{std::make_shared<T>(std::forward<Args>(args)...)} {
  }

  /// Arcs can be used as pointers.
  auto operator->() -> T* {
    return &deref();
  }
  auto operator->() const -> T const* {
    return &deref();
  }

  /// Dereferencing an arc forwards the underlying object.
  template <class Self>
  auto operator*(this Self&& self) -> decltype(auto) {
    return std::forward<Self>(self).deref();
  }

  /// Arcs are implicitly convertible to references.
  explicit(false) operator T&() {
    return deref();
  }
  explicit(false) operator T const&() const {
    return deref();
  }
  explicit(false) operator T&&() && {
    return std::move(*this).deref();
  }

  /// Arcs are callable if the underlying type is.
  template <class Self, class... Args>
    requires std::is_invocable_v<T, Args...>
  auto operator()(this Self&& self, Args&&... args) -> decltype(auto) {
    return std::invoke(std::forward<Self>(self).deref(),
                       std::forward<Args>(args)...);
  }

  /// Comparing arcs compares their value.
  template <class U>
    requires std::equality_comparable_with<T, U>
  auto operator==(Arc<U> const& other) const -> bool {
    return **this == *other;
  }

  /// Arcs can also directly be compared against their inner value.
  template <class U>
    requires(not detail::is_arc_v<std::remove_cvref_t<U>>)
            and std::equality_comparable_with<T, U>
  auto operator==(U const& other) const -> bool {
    return **this == other;
  }

  /// Returns the number of `Arc` instances sharing ownership.
  auto strong_count() const -> size_t {
    return static_cast<size_t>(ptr_.use_count());
  }

private:
  template <class Self>
  auto deref(this Self&& self) -> decltype(auto) {
    TENZIR_ASSERT(self.ptr_);
    return std::forward_like<Self>(*self.ptr_);
  }

  explicit Arc(std::shared_ptr<T> ptr) : ptr_{std::move(ptr)} {
    TENZIR_ASSERT(ptr_);
  }

  std::shared_ptr<T> ptr_;
};

template <class T>
Arc(T) -> Arc<T>;

} // namespace tenzir
