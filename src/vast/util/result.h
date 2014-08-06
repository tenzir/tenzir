#ifndef VAST_UTIL_RESULT_H
#define VAST_UTIL_RESULT_H

#include "vast/optional.h"
#include "vast/util/print.h"

// FIXME: we need to include util/print.h to avoid linker errors in cases when
// somone includes util/result.h but does not include util/print.h beforehand.
// The reason is that util/print.h defines error::render.

namespace vast {
namespace util {

/// A trial that may have an empty (yet valid) result. A result of type `T` is
/// effectively a `trial<optional<T>>` with a more idiomatic interface.
template <typename T>
class result : trial<optional<T>>
{
public:
  using trial<optional<T>>::trial;
  using trial<optional<T>>::error;

  /// Default-constructs an empty-yet-valid result.
  result()
    : trial<optional<T>>{optional<T>{}}
  {
  }

  result(result const&) = default;
  result(result&&) = default;

  /// Constructs a result from an instance of type `T`.
  /// @param x The instance to move.
  result(T x)
    : trial<optional<T>>{std::move(x)}
  {
  }

  result& operator=(T x)
  {
    super() = std::move(x);
    return *this;
  }

  result& operator=(result const&) = default;
  result& operator=(result&&) = default;

  /// Shorthand for ::value.
  T& operator*()
  {
    return value();
  }

  /// Shorthand for ::value.
  T const& operator*() const
  {
    return value();
  }

  /// Shorthand for ::value.
  T* operator->()
  {
    return &value();
  }

  /// Shorthand for ::value.
  T const* operator->() const
  {
    return &value();
  }

  /// Checks whether the result is engaged.
  /// @returns `true` iff the result is engaged.
  explicit operator bool() const
  {
    return engaged();
  }

  /// Retrieves the value of the result.
  /// @returns A mutable reference to the contained value.
  /// @pre `engaged() == true`.
  T& value()
  {
    assert(engaged());
    return *super().value();
  }

  /// Retrieves the value of the result.
  /// @returns The contained value.
  /// @pre `engaged() == true`.
  T const& value() const
  {
    assert(engaged());
    return *super().value();
  }

  /// Checks whether the result is engaged, i.e., if it contains a usable
  /// instance of type `T`.
  ///
  /// @returns `true` if the contained `optional<T>` is engaged.
  bool engaged() const
  {
    return super() && *super();
  }

  /// Checks whether the result is empty, i.e., has not engaged instance of `T`
  /// but has no error either.
  ///
  /// @returns `true` if the contained `optional<T>` is active but disenaged.
  bool empty() const
  {
    return super() && ! *super();
  }

  /// Checks whether the result has failed.
  /// @returns `true` if the underlying ::trial failed.
  bool failed() const
  {
    return ! super();
  }

private:
  trial<optional<T>>& super()
  {
    return static_cast<trial<optional<T>>&>(*this);
  }

  trial<optional<T>> const& super() const
  {
    return static_cast<trial<optional<T>> const&>(*this);
  }
};

} // namespace util

using util::result;

} // namespace vast

#endif
