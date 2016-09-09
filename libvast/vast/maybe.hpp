#ifndef VAST_MAYBE_HPP
#define VAST_MAYBE_HPP

#include <cstddef>
#include <cstdint>
#include <new>
#include <utility>
#include <type_traits>

#include <caf/message.hpp>
#include <caf/none.hpp>

#include <caf/detail/safe_equal.hpp>

#include "vast/util/assert.hpp"
#include "vast/error.hpp"
#include "vast/none.hpp"

namespace vast {

/// Represents a computation returning either `T` or `error`. In addition,
/// `maybe<T>` includes an empty state when default-constructed, as in
/// `std::optional<T>`. In this case a `maybe` instance represents simply
/// `none`. Hence, this type has three possible states:
///
/// 1. A value of `T` is available, no error occurred
///   - `valid()` returns `true`
///   - `empty()` returns `false`
///   - `invalid()` returns `false`
/// 2. No value available, no error occurred
///   - `valid()` returns `false`
///   - `empty()` returns `true`
///   - `invalid()` returns `false`
/// 3. No value available, an error occurred
///   - `valid()` returns `false`
///   - `empty()` returns `false`
///   - `invalid()` returns `true`
template <class T>
class maybe {
public:
  using type = std::remove_reference_t<T>;
  using reference = type&;
  using const_reference = type const&;
  using pointer = type*;
  using const_pointer = type const*;
  using error_type = error;

  /// Type for storing values.
  using storage =
    std::conditional_t<
      std::is_reference<T>::value,
      std::reference_wrapper<type>,
      T
    >;

  /// Creates an instance representing an error.
  maybe(error_type const& x) {
    cr_error(x);
  }

  /// Creates an instance representing an error.
  maybe(error_type&& x) {
    cr_error(std::move(x));
  }

  /// Creates an instance representing a value from `x`.
  template <class U, class = std::enable_if_t<std::is_convertible<U, T>::value>>
  maybe(U&& x) {
    cr_value(std::forward<U>(x));
  }

  template <class U0, class U1, class... Us>
  maybe(U0&& x0, U1&& x1, Us&&... xs) {
    flag_ = available_flag;
    new (&value_) storage(std::forward<U0>(x0), std::forward<U1>(x1),
                          std::forward<Us>(xs)...);
  }

  /// Creates an instance representing an error
  /// from a type offering the free function `make_error`.
  template <
    class E,
    class = std::enable_if_t<
      std::is_same<
        decltype(make_error(std::declval<E const&>())),
        error_type
      >::value
    >
  >
  maybe(E error_enum) : maybe(make_error(error_enum)) {
    // nop
  }

  /// Creates an empty instance.
  maybe() : flag_(empty_flag) {
    // nop
  }

  /// Creates an empty instance.
  maybe(none const&) : flag_(empty_flag) {
    // nop
  }

  maybe(maybe const& other) {
    if (other.valid())
      cr_value(other.value_);
    else
      cr_error(other);
  }

  maybe(maybe&& other) {
    if (other.valid())
      cr_value(std::move(other.value_));
    else
      cr_error(std::move(other));
  }

  template <class U>
  maybe(maybe<U>&& other) {
    static_assert(std::is_convertible<U, T>::value, "U not convertible to T");
    if (other)
      cr_value(std::move(*other));
    else
      cr_error(std::move(other.error()));
  }

  template <class U>
  maybe(maybe<U> const& other) {
    static_assert(std::is_convertible<U, T>::value, "U not convertible to T");
    if (other)
      cr_value(*other);
    else
      cr_error(other.error());
  }

  ~maybe() {
    destroy();
  }

  maybe& operator=(none const&) {
    if (! empty()) {
      destroy();
      flag_ = empty_flag;
    }
    return *this;
  }

  template <class U, class = std::enable_if_t<std::is_convertible<U, T>::value>>
  maybe& operator=(U&& x) {
    if (! valid()) {
      destroy();
      cr_value(std::forward<U>(x));
    } else {
      assign_value(std::forward<U>(x));
    }
    return *this;
  }

  maybe& operator=(error_type const& err) {
    destroy();
    cr_error(err);
    return *this;
  }

  maybe& operator=(error_type&& err) {
    destroy();
    cr_error(std::move(err));
    return *this;
  }

  template <
    class E,
    class = std::enable_if_t<
      std::is_same<
        decltype(make_error(std::declval<E const&>())),
        error_type
      >::value
    >
  >
  maybe& operator=(E error_enum) {
    return *this = make_error(error_enum);
  }

  maybe& operator=(maybe&& other) {
    if (other.valid())
      *this = std::move(*other);
    else
      cr_error(std::move(other));
    return *this;
  }

  maybe& operator=(maybe const& other) {
    if (other.valid())
      *this = *other;
    else
      cr_error(other);
    return *this;
  }

  template <class U>
  maybe& operator=(maybe<U>&& other) {
    static_assert(std::is_convertible<U, T>::value, "U not convertible to T");
    if (other.valid())
      *this = std::move(*other);
    else
      cr_error(std::move(other));
    return *this;
  }

  template <class U>
  maybe& operator=(maybe<U> const& other) {
    static_assert(std::is_convertible<U, T>::value, "U not convertible to T");
    if (other.valid())
      *this = *other;
    else
      cr_error(other);
    return *this;
  }

  /// Queries whether this instance holds a value.
  bool valid() const {
    return flag_ == available_flag;
  }

  /// Returns `available()`.
  explicit operator bool() const {
    return valid();
  }

  /// Returns `! available()`.
  bool operator!() const {
    return ! valid();
  }

  /// Returns the value.
  reference get() {
    VAST_ASSERT(valid());
    return value_;
  }

  /// Returns the value.
  const_reference get() const {
    VAST_ASSERT(valid());
    return value_;
  }

  /// Returns the value.
  reference operator*() {
    return get();
  }

  /// Returns the value.
  const_reference operator*() const {
    return get();
  }

  /// Returns a pointer to the value.
  pointer operator->() {
    return &get();
  }

  /// Returns a pointer to the value.
  const_pointer operator->() const {
    return &get();
  }

  /// Returns whether this objects holds neither a value nor an actual error.
  bool empty() const {
    return flag_ == empty_flag;
  }

  bool invalid() const {
    return (flag_ & error_code_mask) != 0;
  }

  /// Creates an error object.
  error_type error() const {
    if (valid())
      return {};
    if (has_error_context())
      return {error_code(), extended_error_->first, extended_error_->second};
    return {error_code(), error_category_};
  }

  uint8_t error_code() const {
    return static_cast<uint8_t>(flag_ & error_code_mask);
  }

  caf::atom_value error_category() const {
    if (valid())
      return caf::atom("");
    if (has_error_context())
      return extended_error_->first;
    return error_category_;
  }

  // FIXME: monadically chain return values of inspector invocations.
  template <class Inspector>
  friend auto inspect(Inspector& f, maybe& m) {
    auto save = [&] {
      uint8_t flag = m.empty() ? 0 : (m.valid() ? 1 : 2);
      f(flag);
      if (m.valid())
        f(*m);
      else if (m.invalid()) {
        auto e = m.error();
        f(e);
      }
      return caf::none;
    };
    auto load = [&] {
      uint8_t flag;
      f(flag);
      switch (flag) {
        case 1: {
          T value;
          f(value);
          m = std::move(value);
          break;
        }
        case 2: {
          vast::error e;
          f(e);
          m = std::move(e);
          break;
        }
        default:
          m = nil;
      }
      return caf::none;
    };
    return f(caf::meta::save_callback(save),
             caf::meta::load_callback(load));
  }

private:
  bool has_error_context() const {
    return (flag_ & error_context_mask) != 0;
  }

  void destroy() {
    if (valid())
      value_.~storage();
    else if (has_error_context())
      delete extended_error_;
  }

  template <class V>
  void assign_value(V&& x) {
    using x_type = std::remove_reference_t<V>;
    using fwd_type =
      std::conditional_t<
        std::is_rvalue_reference<decltype(x)>{} && ! std::is_reference<T>{},
        x_type&&,
        x_type&
      >;
    value_ = static_cast<fwd_type>(x);
  }

  template <class V>
  void cr_value(V&& x) {
    using x_type = std::remove_reference_t<V>;
    using fwd_type =
      std::conditional_t<
        std::is_rvalue_reference<decltype(x)>{} && ! std::is_reference<T>{},
        x_type&&,
        x_type&
      >;
    flag_ = available_flag;
    new (&value_) storage(static_cast<fwd_type>(x));
  }

  template <class U>
  void cr_error(maybe<U> const& other) {
    flag_ = other.flag_;
    if (has_error_context())
      extended_error_ = new extended_error(*other.extended_error_);
    else
      error_category_ = other.error_category_;
  }

  template <class U>
  void cr_error(maybe<U>&& other) {
    flag_ = other.flag_;
    if (has_error_context())
      extended_error_ = other.extended_error_;
    else
      error_category_ = other.error_category_;
    other.flag_ = empty_flag; // take ownership of extended_error_
  }

  void cr_error(error_type const& x) {
    flag_ = x.code() & error_code_mask;
    if (x.context().empty()) {
      error_category_ = x.category();
      return;
    }
    extended_error_ = new extended_error(x.category(), x.context());
    flag_ |= error_context_mask;
  }

  void cr_error(error_type&& x) {
    flag_ = x.code() & error_code_mask;
    if (x.context().empty()) {
      error_category_ = x.category();
      return;
    }
    extended_error_ = new extended_error(x.category(), x.context());
    flag_ |= error_context_mask;
  }

  static constexpr uint32_t empty_flag = 0x00000000;
  static constexpr uint32_t available_flag = 0x80000000;
  static constexpr uint32_t error_code_mask = 0x000000FF;
  static constexpr uint32_t error_context_mask = 0x40000000;

  using extended_error = std::pair<caf::atom_value, caf::message>;

  // The flag has 3 segments:
  // - 1 bit for the availability flag,
  // - 1 bit to indicate a non-empty error context
  // - 8 bit for the error code
  uint32_t flag_;
  union {
    storage value_;                  // if flag == available_flag
    caf::atom_value error_category_; // if (flag & error_context_mask) == 0
    extended_error* extended_error_; // if (flag & error_context_mask) != 0
  };
};

/// Represents a computation performing side effects only, and maybe returns an
/// `error`. This specialization collapses the notions of *valid* and *empty*,
/// reducing `maybe<void>` to a two-state structure: it represents a
/// valid/empty instance upon "success" and an ::error upon failure. In a
/// boolean context, `maybe<void>` evaluates to `true` *iff* it contains
/// no error.
template <>
class maybe<void> {
public:
  using type = none;
  using reference = type const&;
  using const_reference = type const&;
  using pointer = type const*;
  using const_pointer = type const*;
  using error_type = error;

  maybe() = default;

  maybe(none const&) {
    // nop
  }

  maybe(error_type err) : error_(std::move(err)) {
    // nop
  }

  template <
    class E,
    class = std::enable_if_t<
      std::is_same<
        decltype(make_error(std::declval<E const&>())),
        error_type
      >::value
    >
  >
  maybe(E error_code) : error_(make_error(error_code)) {
    // nop
  }

  maybe& operator=(none const&) {
    error_.clear();
    return *this;
  }

  maybe& operator=(error_type err) {
    error_ = std::move(err);
    return *this;
  }

  template <
    class E,
    class = std::enable_if_t<
      std::is_same<
        decltype(make_error(std::declval<E const&>())),
        error_type
      >::value
    >
  >
  maybe& operator=(E error_code) {
    return *this = make_error(error_code);
  }

  bool valid() const {
    return ! error();
  }

  explicit operator bool() const {
    return valid();
  }

  bool operator!() const {
    return ! valid();
  }

  reference get() {
    VAST_ASSERT(! "should never be called");
    return nil;
  }

  const_reference get() const {
    VAST_ASSERT(! "should never be called");
    return nil;
  }

  reference operator*() {
    return get();
  }

  const_reference operator*() const {
    return get();
  }

  pointer operator->() {
    return &get();
  }

  const_pointer operator->() const {
    return &get();
  }

  bool empty() const {
    return ! error();
  }

  const error_type& error() const {
    return error_;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, maybe& m) {
    return f(m.error_);
  }

private:
  error_type error_;
};

/// Allows element-wise access of STL-compatible tuples.
/// @relates maybe
template <size_t X, class T>
maybe<typename std::tuple_element<X, T>::type&> get(maybe<T>& xs) {
  if (xs)
    return std::get<X>(*xs);
  return xs.error();
}

/// Allows element-wise access of STL-compatible tuples.
/// @relates maybe
template <size_t X, class T>
maybe<typename std::tuple_element<X, T>::type const&> get(maybe<T> const& xs) {
  if (xs)
    return std::get<X>(*xs);
  return xs.error();
}

// -- comparison with other maybe instance -----------------------------------

/// Returns `true` if both objects represent either the same
/// value or the same error, `false` otherwise.
/// @relates maybe
template <class T, class U>
bool operator==(maybe<T> const& x, maybe<U> const& y) {
  if (x)
    return (y) ? caf::detail::safe_equal(*x, *y) : false;
  if (x.empty() && y.empty())
    return true;
  if (! y)
    return x.error_code() == y.error_code()
           && x.error_category() == y.error_category();
  return false;
}

/// Returns `true` if the objects represent different
/// values or errors, `false` otherwise.
/// @relates maybe
template <class T, class U>
bool operator!=(maybe<T> const& x, maybe<U> const& y) {
  return !(x == y);
}

// -- comparison with values -------------------------------------------------

/// Returns `true` if `lhs` is available and its value is equal to `rhs`.
template <class T, class U>
bool operator==(maybe<T> const& x, U const& y) {
  return (x) ? *x == y : false;
}

/// Returns `true` if `rhs` is available and its value is equal to `lhs`.
/// @relates maybe
template <class T, class U>
bool operator==(const T& x, maybe<U> const& y) {
  return y == x;
}

/// Returns `true` if `lhs` is not available or its value is not equal to `rhs`.
/// @relates maybe
template <class T, class U>
bool operator!=(maybe<T> const& x, U const& y) {
  return !(x == y);
}

/// Returns `true` if `rhs` is not available or its value is not equal to `lhs`.
/// @relates maybe
template <class T, class U>
bool operator!=(T const& x, maybe<U> const& y) {
  return !(x == y);
}

// -- comparison with errors -------------------------------------------------

/// Returns `! val.available() && val.error() == err`.
/// @relates maybe
template <class T>
bool operator==(maybe<T> const& x, error const& y) {
  return x.invalid() && y.compare(x.error_code(), x.error_category()) == 0;
}

/// Returns `! val.available() && val.error() == err`.
/// @relates maybe
template <class T>
bool operator==(error const& x, maybe<T> const& y) {
  return y == x;
}

/// Returns `val.available() || val.error() != err`.
/// @relates maybe
template <class T>
bool operator!=(maybe<T> const& x, error const& y) {
  return ! (x == y);
}

/// Returns `val.available() || val.error() != err`.
/// @relates maybe
template <class T>
bool operator!=(error const& x, maybe<T> const& y) {
  return ! (y == x);
}

// -- comparison with none_t -------------------------------------------------

/// Returns `val.empty()`.
/// @relates maybe
template <class T>
bool operator==(maybe<T> const& x, none const&) {
  return x.empty();
}

/// Returns `val.empty()`.
/// @relates maybe
template <class T>
bool operator==(none const&, maybe<T> const& x) {
  return x.empty();
}

/// Returns `! val.empty()`.
/// @relates maybe
template <class T>
bool operator!=(maybe<T> const& x, none const&) {
  return ! x.empty();
}

/// Returns `! val.empty()`.
/// @relates maybe
template <class T>
bool operator!=(none const&, maybe<T> const& x) {
  return ! x.empty();
}

} // namespace vast

#endif // VAST_MAYBE_HPP
