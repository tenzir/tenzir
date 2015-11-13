#ifndef VAST_UTIL_RESULT_H
#define VAST_UTIL_RESULT_H

#include "vast/maybe.h"
#include "vast/trial.h"
#include "vast/util/assert.h"

namespace vast {

/// A trial that may have an empty (yet valid) result. A result of type `T` is
/// effectively a `trial<maybe<T>>` with a more idiomatic interface.
template <typename T>
class result : trial<maybe<T>> {
public:
  using trial<maybe<T>>::trial;
  using trial<maybe<T>>::error;

  /// Default-constructs an empty-yet-valid result.
  result() : trial<maybe<T>>{maybe<T>{}} {
  }

  result(result const&) = default;
  result(result&&) = default;

  /// Constructs a result from an instance of type `T`.
  /// @param x The instance to move.
  result(T x) : trial<maybe<T>>{std::move(x)} {
  }

  result& operator=(T x) {
    super() = std::move(x);
    return *this;
  }

  result& operator=(result const&) = default;
  result& operator=(result&&) = default;

  /// Shorthand for ::value.
  T& operator*() {
    return value();
  }

  /// Shorthand for ::value.
  T const& operator*() const {
    return value();
  }

  /// Shorthand for ::value.
  T* operator->() {
    return &value();
  }

  /// Shorthand for ::value.
  T const* operator->() const {
    return &value();
  }

  /// Checks whether the result is engaged.
  /// @returns `true` iff the result is engaged.
  explicit operator bool() const {
    return engaged();
  }

  /// Retrieves the value of the result.
  /// @returns A mutable reference to the contained value.
  /// @pre `engaged() == true`.
  T& value() {
    VAST_ASSERT(engaged());
    return *super().value();
  }

  /// Retrieves the value of the result.
  /// @returns The contained value.
  /// @pre `engaged() == true`.
  T const& value() const {
    VAST_ASSERT(engaged());
    return *super().value();
  }

  /// Checks whether the result is engaged, i.e., if it contains a usable
  /// instance of type `T`.
  /// @returns `true` if the contained `maybe<T>` is engaged.
  bool engaged() const {
    return super() && *super();
  }

  /// Checks whether the result is empty, i.e., has not engaged instance of `T`
  /// but has no error either.
  /// @returns `true` if the contained `maybe<T>` is active but disenaged.
  bool empty() const {
    return super() && !*super();
  }

  /// Checks whether the result has failed.
  /// @returns `true` if the underlying ::trial failed.
  bool failed() const {
    return !super();
  }

private:
  trial<maybe<T>>& super() {
    return static_cast<trial<maybe<T>>&>(*this);
  }

  trial<maybe<T>> const& super() const {
    return static_cast<trial<maybe<T>> const&>(*this);
  }
};

} // namespace vast

#endif
