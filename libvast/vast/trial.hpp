#ifndef VAST_TRIAL_HPP
#define VAST_TRIAL_HPP

#include <memory>
#include <type_traits>

#include "vast/error.hpp"
#include "vast/util/assert.hpp"

namespace vast {

/// Represents the result of a computation which can either complete
/// successfully with an instance of type `T` or fail with an ::error.
/// @tparam The type of the result.
template <typename T>
class trial {
public:
  /// Constructs a trial from an instance of `T`.
  /// @param x An instatnce of type .`T`
  /// @post The trial is *engaged*, i.e., `*this == true`.
  trial(T const& x) : engaged_{true} {
    new (&value_) T(x);
  }

  trial(T&& x) noexcept(std::is_nothrow_move_constructible<T>::value)
    : engaged_{true} {
    new (&value_) T(std::move(x));
  }

  /// Constructs a trial from an error.
  /// @param e The error.
  /// @post The trial is *disengaged*, i.e., `*this == false`.
  trial(vast::error e) : engaged_{false} {
    new (&error_) vast::error{std::move(e)};
  }

  /// Copy-constructs a trial.
  /// @param other The other trial.
  trial(trial const& other) {
    construct(other);
  }

  /// Move-constructs a trial.
  /// @param other The other trial.
  trial(trial&& other) noexcept {
    construct(std::move(other));
  }

  ~trial() {
    destroy();
  }

  /// Assigns another trial to this instance.
  /// @param other The RHS of the assignment.
  /// @returns A reference to `*this`.
  trial& operator=(trial const& other) {
    destroy();
    construct(other);
    return *this;
  }

  trial& operator=(trial&& other) noexcept(
    std::is_nothrow_move_constructible<T>::value) {
    destroy();
    construct(std::move(other));
    return *this;
  }

  /// Assigns a trial by constructing an instance of `T` from the RHS
  /// expression.
  /// @param args The arguments to forward to `T`'s constructor.
  /// @returns A reference to `*this`.
  trial& operator=(T const& x) {
    destroy();
    engaged_ = true;
    new (&value_) T(x);
    return *this;
  }

  trial&
  operator=(T&& x) noexcept(std::is_nothrow_move_constructible<T>::value) {
    destroy();
    engaged_ = true;
    new (&value_) T(std::move(x));
    return *this;
  }

  /// Assigns an error to this trial instance.
  /// @param e The error.
  /// @returns A reference to `*this`.
  /// @post The trial is *disengaged*, i.e., `*this == false`.
  trial& operator=(vast::error e) {
    destroy();
    engaged_ = false;
    new (&value_) vast::error{std::move(e)};
    return *this;
  }

  /// Checks whether the trial is engaged.
  /// @returns `true` iff the trial is engaged.
  explicit operator bool() const {
    return engaged_;
  }

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

  /// Retrieves the value of the trial.
  /// @returns A mutable reference to the contained value.
  /// @pre `*this == true`.
  T& value() {
    VAST_ASSERT(engaged_);
    return value_;
  }

  /// Retrieves the value of the trial.
  /// @returns The contained value.
  /// @pre `*this == true`.
  T const& value() const {
    VAST_ASSERT(engaged_);
    return value_;
  }

  /// Retrieves the error of the trial.
  /// @returns The contained error.
  /// @pre `*this == false`.
  vast::error const& error() const {
    VAST_ASSERT(!engaged_);
    return error_;
  }

  /// Retrieves the error of the trial.
  /// @returns The contained error.
  /// @pre `*this == false`.
  vast::error& error() {
    VAST_ASSERT(!engaged_);
    return error_;
  }

private:
  void construct(trial const& other) {
    if (other.engaged_) {
      engaged_ = true;
      new (&value_) T(other.value_);
    } else {
      engaged_ = false;
      new (&error_) vast::error{other.error_};
    }
  }

  void construct(trial&& other) noexcept(
    std::is_nothrow_move_constructible<T>::value) {
    if (other.engaged_) {
      engaged_ = true;
      new (&value_) T(std::move(other.value_));
    } else {
      engaged_ = false;
      new (&error_) vast::error{std::move(other.error_)};
    }
  }

  void destroy() {
    if (engaged_)
      value_.~T();
    else
      error_.~error();
  }

  union {
    T value_;
    vast::error error_;
  };

  bool engaged_;
};

/// The pattern `trial<void>` shall be used for functions that may generate an
/// error but would otherwise return `bool`.
template <>
class trial<void> {
public:
  trial() = default;

  trial(vast::error e) : error_{std::make_unique<vast::error>(std::move(e))} {
  }

  trial(trial const& other) : error_{other.error_.get()} {
  }

  trial(trial&& other) noexcept : error_{std::move(other.error_)} {
  }

  trial& operator=(trial const& other) {
    error_.reset(other.error_.get());
    return *this;
  }

  trial& operator=(trial&& other) noexcept {
    error_ = std::move(other.error_);
    return *this;
  }

  explicit operator bool() const {
    return !error_;
  }

  vast::error const& error() const {
    VAST_ASSERT(error_);
    return *error_;
  }

private:
  std::unique_ptr<vast::error> error_;
};

/// Represents success in a `trial<void>` scenario.
/// @relates trial
trial<void> const nothing = {};

} // namespace vast

#endif
