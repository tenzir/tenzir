//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/panic.hpp"
#include "tenzir/result.hpp"
#include "tenzir/try.hpp"
#include "tenzir/variant_traits.hpp"

#include <caf/inspector_access.hpp>
#include <fmt/format.h>

#include <compare>
#include <concepts>
#include <optional>
#include <source_location>
#include <type_traits>
#include <utility>

namespace tenzir {

/// Tag type representing the absence of a value.
struct None {
  friend auto operator==(None, None) -> bool = default;
};

template <class T>
class Option;

namespace detail {

template <class T>
inline constexpr auto is_option_v = false;

template <class T>
inline constexpr auto is_option_v<Option<T>> = true;

// std::optional interoperability.
// NOLINTBEGIN(custom-prefer-option, custom-prefer-option-ctad)
template <class T>
inline constexpr auto is_std_optional_v = false;

template <class T>
inline constexpr auto is_std_optional_v<std::optional<T>> = true;

template <class T, class U>
concept EqualityComparable = requires(T const& a, U const& b) {
  { a == b } -> std::convertible_to<bool>;
};

template <class T, class U>
concept ThreeWayComparable = requires(T const& a, U const& b) {
  { a <=> b };
};

/// Storage backend for `Option<T>` when `T` is a value type.
template <class T>
class OptionStorage {
public:
  OptionStorage() noexcept = default;
  constexpr explicit OptionStorage(std::optional<T> opt) noexcept(
    std::is_nothrow_move_constructible_v<std::optional<T>>)
    : inner_{std::move(opt)} {
  }
  constexpr auto is_some() const noexcept -> bool {
    return inner_.has_value();
  }
  template <class Self>
  // NOLINTNEXTLINE(cppcoreguidelines-missing-std-forward)
  constexpr auto get(this Self&& self) -> decltype(auto) {
    return std::forward_like<Self>(*self.inner_);
  }
  auto reset() noexcept -> void {
    inner_.reset();
  }
  template <class... Args>
  auto emplace(Args&&... args) -> T& {
    return inner_.emplace(std::forward<Args>(args)...);
  }

private:
  std::optional<T> inner_;
};

/// Storage backend for `Option<T&>` (reference semantics, rebinding).
template <class T>
class OptionStorage<T&> {
public:
  OptionStorage() noexcept = default;
  explicit OptionStorage(T& ref) noexcept : ptr_{&ref} {
  }
  auto is_some() const noexcept -> bool {
    return ptr_ != nullptr;
  }
  auto get() -> T& {
    return *ptr_;
  }
  auto get() const -> T const& {
    return *ptr_;
  }
  auto reset() noexcept -> void {
    ptr_ = nullptr;
  }
  template <class U>
    requires std::convertible_to<U&, T&>
  auto emplace(U& ref) -> T& {
    ptr_ = &ref;
    return *ptr_;
  }

private:
  T* ptr_ = nullptr;
};

} // namespace detail

/// An optional type that supports `T&` and monadic operations.
///
/// By default, all access is checked.
template <class T>
class Option {
  using Storage = detail::OptionStorage<T>;
  using Value = std::remove_reference_t<T>;

public:
  using value_type = std::remove_reference_t<T>;

  // -- Construction -----------------------------------------------------------

  /// Constructs an empty option.
  Option() noexcept = default;

  /// Constructs an empty option from `None`.
  constexpr explicit(false) Option(None) noexcept {
  }

  /// Constructs from a value (non-reference `T` only).
  template <class U>
    requires(not std::is_reference_v<T>
             and not std::same_as<std::remove_cvref_t<U>, None>
             and not std::same_as<std::remove_cvref_t<U>, Option>
             and not std::same_as<std::remove_cvref_t<U>, std::optional<T>>
             and std::constructible_from<T, U>)
  constexpr explicit(not std::convertible_to<U, T>) Option(U&& value) noexcept(
    std::is_nothrow_constructible_v<T, U&&>
    and std::is_nothrow_constructible_v<Storage, std::optional<T>>)
    : storage_{std::optional<T>{std::in_place, std::forward<U>(value)}} {
  }

  /// Constructs an engaged option in-place.
  template <class... Args>
    requires(not std::is_reference_v<T> and std::constructible_from<T, Args...>)
  explicit Option(std::in_place_t, Args&&... args) noexcept(
    std::is_nothrow_constructible_v<T, Args&&...>
    and std::is_nothrow_constructible_v<Storage, std::optional<T>>)
    : storage_{std::optional<T>{std::in_place, std::forward<Args>(args)...}} {
  }

  /// Constructs from another option while preserving the outer shape.
  template <class U>
    requires(not std::is_reference_v<T>
             and not detail::is_option_v<std::remove_cvref_t<T>>
             and std::constructible_from<T, U const&>)
  explicit(not std::convertible_to<U const&, T>)
    Option(Option<U> const& other) noexcept(
      std::is_nothrow_constructible_v<T, U const&>
      and std::is_nothrow_constructible_v<Storage, std::optional<T>>)
    : storage_{other.is_some() ? std::optional<T>{std::in_place, *other}
                               : std::optional<T>{}} {
  }

  /// Constructs from another option while preserving the outer shape.
  template <class U>
    requires(not std::is_reference_v<T>
             and not detail::is_option_v<std::remove_cvref_t<T>>
             and std::constructible_from<T, U>)
  explicit(not std::convertible_to<U, T>) Option(Option<U>&& other) noexcept(
    std::is_nothrow_constructible_v<T, U>
    and std::is_nothrow_constructible_v<Storage, std::optional<T>>)
    : storage_{other.is_some()
                 ? std::optional<T>{std::in_place, *std::move(other)}
                 : std::optional<T>{}} {
  }

  /// Constructs from a reference (reference `T` only).
  template <class U = std::remove_reference_t<T>>
    requires(std::is_reference_v<T> and std::convertible_to<U&, T>)
  explicit(false)
    Option(U& ref) noexcept(std::is_nothrow_constructible_v<Storage, U&>)
    : storage_{ref} {
  }

  /// Constructs from `std::nullopt`.
  explicit(false) Option(std::nullopt_t) noexcept {
  }

  /// Constructs from a `std::optional` (non-reference `T` only).
  template <class U = T>
    requires(not std::is_reference_v<T>)
  explicit(false) Option(std::optional<U> opt) noexcept(
    std::is_nothrow_constructible_v<Storage, std::optional<U>>)
    : storage_{std::move(opt)} {
  }

  Option(Option const&) = default;
  Option(Option&&) noexcept(std::is_nothrow_move_constructible_v<Storage>)
    = default;

  // -- Assignment -------------------------------------------------------------

  /// Resets to empty.
  auto operator=(None) noexcept(noexcept(storage_.reset())) -> Option& {
    storage_.reset();
    return *this;
  }

  auto operator=(Option const&) -> Option& = default;
  auto operator=(Option&&) -> Option& = default;

  /// Assigns a value (non-reference `T` only).
  template <class U = T>
    requires(not std::is_reference_v<T> and std::constructible_from<T, U>)
  auto operator=(U&& value) -> Option& {
    storage_.emplace(std::forward<U>(value));
    return *this;
  }

  /// Assigns from a `std::optional` (non-reference `T` only).
  template <class U = T>
    requires(not std::is_reference_v<T> and std::constructible_from<T, U>)
  auto operator=(std::optional<U> other) -> Option& {
    if (other) {
      storage_.emplace(*std::move(other));
    } else {
      storage_.reset();
    }
    return *this;
  }

  /// Rebinds to a reference (reference `T` only).
  template <class U = std::remove_reference_t<T>>
    requires(std::is_reference_v<T> and std::convertible_to<U&, T&>)
  auto operator=(U& ref) -> Option& {
    storage_.emplace(ref);
    return *this;
  }

  // -- Observers --------------------------------------------------------------

  /// Returns whether the option contains a value.
  constexpr explicit operator bool() const noexcept {
    return is_some();
  }

  /// Returns whether the option contains a value.
  constexpr auto is_some() const noexcept -> bool {
    return storage_.is_some();
  }

  /// Returns whether the option contains a value.
  auto has_value() const noexcept -> bool {
    return is_some();
  }

  /// Returns whether the option is empty.
  constexpr auto is_none() const noexcept -> bool {
    return not is_some();
  }

  /// Resets to empty.
  auto reset() noexcept(noexcept(storage_.reset())) -> void {
    storage_.reset();
  }

  /// Constructs/rebinds the contained value in-place.
  template <class... Args>
    requires(not std::is_reference_v<T> and std::constructible_from<T, Args...>)
  auto emplace(Args&&... args) -> Value& {
    return storage_.emplace(std::forward<Args>(args)...);
  }

  /// Rebinds the contained reference in-place.
  template <class U = std::remove_reference_t<T>>
    requires(std::is_reference_v<T> and std::convertible_to<U&, T>)
  auto emplace(U& ref) -> Value& {
    return storage_.emplace(ref);
  }

  /// Returns `true` if the option has a value and the predicate returns `true`.
  template <class F>
    requires std::predicate<F, Value const&>
  auto is_some_and(F&& pred) const -> bool {
    return is_some() and std::invoke(std::forward<F>(pred), storage_.get());
  }

  // -- Checked access (panics on None) ----------------------------------------

  /// Accesses the contained value. Panics if empty.
  template <class Self>
  constexpr auto operator*(this Self&& self) -> decltype(auto) {
    if (not self.is_some()) [[unlikely]] {
      panic("called Option::operator* on a None value");
    }
    return std::forward<Self>(self).storage_.get();
  }

  /// Pointer-style access. Panics if empty.
  auto operator->() -> Value* {
    if (not is_some()) [[unlikely]] {
      panic("called Option::operator-> on a None value");
    }
    return std::addressof(storage_.get());
  }

  /// Pointer-style access. Panics if empty.
  auto operator->() const -> Value const* {
    if (not is_some()) [[unlikely]] {
      panic("called Option::operator-> on a None value");
    }
    return std::addressof(storage_.get());
  }

  /// Unwraps the contained value. Panics if empty.
  template <class Self>
  auto unwrap(this Self&& self, std::source_location loc
                                = std::source_location::current())
    -> decltype(auto) {
    if (not self.is_some()) [[unlikely]] {
      panic_at(loc, "called Option::unwrap() on a None value");
    }
    return std::forward<Self>(self).storage_.get();
  }

  /// Accesses the contained value. Panics if empty.
  template <class Self>
  auto value(this Self&& self, std::source_location loc
                               = std::source_location::current())
    -> decltype(auto) {
    return std::forward<Self>(self).unwrap(loc);
  }

  /// Unwraps the contained value with a custom panic message.
  template <class Self>
  auto expect(this Self&& self, std::string_view msg,
              std::source_location loc = std::source_location::current())
    -> decltype(auto) {
    if (not self.is_some()) [[unlikely]] {
      panic_at(loc, "{}", msg);
    }
    return std::forward<Self>(self).storage_.get();
  }

  // -- Unchecked access -------------------------------------------------------

  /// Unwraps without checking. Undefined behavior if empty.
  template <class Self>
  auto unwrap_unchecked(this Self&& self) -> decltype(auto) {
    return std::forward<Self>(self).storage_.get();
  }

  // -- Unwrapping with fallback -----------------------------------------------

  /// Returns the contained value, or `fallback` if empty.
  template <class Self, class U>
    requires std::convertible_to<U, T>
  auto unwrap_or(this Self&& self, U&& fallback) -> Value {
    if (self.is_some()) {
      return std::forward<Self>(self).storage_.get();
    }
    return static_cast<Value>(std::forward<U>(fallback));
  }

  /// Returns the contained value, or `fallback` if empty.
  template <class Self, class U>
    requires std::convertible_to<U, T>
  auto value_or(this Self&& self, U&& fallback) -> Value {
    return std::forward<Self>(self).unwrap_or(std::forward<U>(fallback));
  }

  /// Converts to `std::optional` for third-party API boundaries.
  auto to_std() const& -> std::optional<Value>
    requires(not std::is_reference_v<T> and std::copy_constructible<T>)
  {
    if (is_some()) {
      return std::optional<Value>{std::in_place, storage_.get()};
    }
    return std::nullopt; // NOLINT(custom-prefer-none): interoperability.
  }

  /// Converts to `std::optional` for third-party API boundaries.
  auto to_std() && -> std::optional<Value>
    requires(not std::is_reference_v<T> and std::move_constructible<T>)
  {
    if (is_some()) {
      return std::optional<Value>{std::in_place, *std::move(*this)};
    }
    return std::nullopt; // NOLINT(custom-prefer-none): interoperability.
  }

  /// Returns the contained value, or computes it from `f` if empty.
  template <class Self, class F>
    requires std::invocable<F>
  auto unwrap_or_else(this Self&& self, F&& f) -> Value {
    if (self.is_some()) {
      return std::forward<Self>(self).storage_.get();
    }
    return std::invoke(std::forward<F>(f));
  }

  /// Returns the contained value, or a default-constructed one if empty.
  template <class Self>
    requires(not std::is_reference_v<T> and std::default_initializable<T>)
  auto unwrap_or_default(this Self&& self) -> Value {
    if (self.is_some()) {
      return std::forward<Self>(self).storage_.get();
    }
    return Value{};
  }

  /// Converts `Option<T>` to `Result<T, E>`, mapping `None` to `err`.
  template <class Self, class E>
  auto ok_or(this Self&& self, E&& err)
    -> Result<Value, std::remove_cvref_t<E>> {
    if (self.is_some()) {
      return std::forward<Self>(self).storage_.get();
    }
    return Err{std::forward<E>(err)};
  }

  // -- Transformations --------------------------------------------------------

  /// Applies `f` to the contained value, returning `Option<U>`.
  template <class Self, class F>
  auto map(this Self&& self, F&& f) -> Option<
    std::invoke_result_t<F, decltype(std::forward<Self>(self).storage_.get())>> {
    if (self.is_some()) {
      return Option<std::invoke_result_t<
        F, decltype(std::forward<Self>(self).storage_.get())>>{
        std::invoke(std::forward<F>(f),
                    std::forward<Self>(self).storage_.get())};
    }
    return None{};
  }

  /// Applies `f` to the contained value, returning `Option<U>`.
  template <class Self, class F>
  auto transform(this Self&& self, F&& f) {
    return std::forward<Self>(self).map(std::forward<F>(f));
  }

  /// Applies `f` (which returns `Option<U>`) to the contained value.
  template <class Self, class F>
  auto and_then(this Self&& self, F&& f)
    -> std::invoke_result_t<F,
                            decltype(std::forward<Self>(self).storage_.get())> {
    if (self.is_some()) {
      return std::invoke(std::forward<F>(f),
                         std::forward<Self>(self).storage_.get());
    }
    return None{};
  }

  /// Returns `*this` if it has a value, otherwise calls `f`.
  template <class Self, class F>
    requires std::invocable<F>
  auto or_else(this Self&& self, F&& f) -> Option {
    if (self.is_some()) {
      return std::forward<Self>(self);
    }
    return std::invoke(std::forward<F>(f));
  }

  /// Keeps the value only if the predicate returns `true`.
  template <class Self, class F>
    requires std::predicate<F, Value const&>
  auto filter(this Self&& self, F&& pred) -> Option {
    if (self.is_some()
        and std::invoke(std::forward<F>(pred),
                        std::as_const(self.storage_.get()))) {
      return std::forward<Self>(self);
    }
    return None{};
  }

  // -- Combinators ------------------------------------------------------------

  /// Flattens `Option<Option<U>>` into `Option<U>`.
  template <class Self>
    requires requires {
      typename std::remove_cvref_t<
        decltype(std::forward<Self>(std::declval<Self>()).storage_.get())>::Value;
    }
  auto flatten(this Self&& self) {
    using Inner
      = std::remove_cvref_t<decltype(std::forward<Self>(self).storage_.get())>;
    if (self.is_some()) {
      return Inner{std::forward<Self>(self).storage_.get()};
    }
    return Inner{None{}};
  }

  /// Zips two options into `Option<std::pair<T, U>>`.
  template <class Self, class U>
  auto zip(this Self&& self, Option<U> other)
    -> Option<std::pair<Value, std::remove_reference_t<U>>> {
    if (self.is_some() and other.is_some()) {
      return Option<std::pair<Value, std::remove_reference_t<U>>>{
        std::pair{std::forward<Self>(self).storage_.get(), *std::move(other)}};
    }
    return None{};
  }

  // -- Comparison -------------------------------------------------------------

  /// Two options are equal if both are None, or both are Some with equal values.
  template <class U>
    requires detail::EqualityComparable<Value, std::remove_reference_t<U>>
  friend auto operator==(Option const& lhs, Option<U> const& rhs) -> bool {
    if (lhs.is_some() != rhs.is_some()) {
      return false;
    }
    if (lhs.is_none()) {
      return true;
    }
    return *lhs == *rhs;
  }

  /// An option equals None iff it is empty.
  friend auto operator==(Option const& lhs, None) -> bool {
    return lhs.is_none();
  }

  /// Compares against a `std::optional` at third-party API boundaries.
  template <class U>
    requires detail::EqualityComparable<Value, U>
  auto operator==(std::optional<U> const& rhs) const -> bool {
    if (is_some() != rhs.has_value()) {
      return false;
    }
    return is_none() or **this == *rhs;
  }

  /// An option equals a value iff it is Some and the values are equal.
  template <class U>
    requires(not std::same_as<std::remove_cvref_t<U>, None>
             and not detail::is_option_v<std::remove_cvref_t<U>>
             and not detail::is_std_optional_v<std::remove_cvref_t<U>>
             and detail::EqualityComparable<Value, U>)
  auto operator==(U const& rhs) const -> bool {
    return is_some() and **this == rhs;
  }

  /// Orders two options. None is less than any Some.
  template <class U>
    requires detail::ThreeWayComparable<Value, std::remove_reference_t<U>>
  friend auto operator<=>(Option const& lhs, Option<U> const& rhs) {
    if (lhs.is_some() and rhs.is_some()) {
      return *lhs <=> *rhs;
    }
    return lhs.is_some() <=> rhs.is_some();
  }

  /// None is less than Some.
  friend auto operator<=>(Option const& lhs, None) -> std::strong_ordering {
    return lhs.is_some() <=> false;
  }

  /// Compares directly against a value. None is less than any value.
  template <class U>
    requires(not std::same_as<std::remove_cvref_t<U>, None>
             and not detail::is_option_v<std::remove_cvref_t<U>>
             and detail::ThreeWayComparable<Value, U>)
  auto operator<=>(U const& rhs) const
    -> std::common_comparison_category_t<decltype(std::declval<Value const&>()
                                                  <=> std::declval<U const&>()),
                                         std::strong_ordering> {
    using result_type = std::common_comparison_category_t<
      decltype(std::declval<Value const&>() <=> std::declval<U const&>()),
      std::strong_ordering>;
    if (is_some()) {
      return static_cast<result_type>(**this <=> rhs);
    }
    return static_cast<result_type>(std::strong_ordering::less);
  }

private:
  template <class>
  friend class Option;
  template <concepts::unqualified>
  friend class variant_traits;

  Storage storage_;
};
// NOLINTEND(custom-prefer-option, custom-prefer-option-ctad)

/// Unwraps an option and panics if it is empty.
template <class T>
[[nodiscard]] auto check(Option<T> result, std::source_location location
                                           = std::source_location::current())
  -> T {
  return std::move(result).unwrap(location);
}

/// Deduction guide: deduces values, not references.
template <class T>
Option(T) -> Option<T>;

// -- variant_traits -----------------------------------------------------------

template <class T>
class variant_traits<Option<T>> {
public:
  static constexpr auto count = size_t{2};

  static constexpr auto index(Option<T> const& x) -> size_t {
    return x.is_some() ? 0 : 1;
  }

  template <size_t I>
  static constexpr auto get(Option<T> const& x) -> decltype(auto) {
    if constexpr (I == 0) {
      return x.storage_.get();
    } else {
      static_assert(I == 1);
      return None{};
    }
  }
};

} // namespace tenzir

// -- tryable ------------------------------------------------------------------

template <class T>
struct tenzir::tryable<tenzir::Option<T>> {
  static auto is_success(tenzir::Option<T> const& x) -> bool {
    return x.is_some();
  }

  static auto get_success(tenzir::Option<T>&& x) -> std::remove_reference_t<T> {
    return *std::move(x);
  }

  static auto get_error(
    tenzir::Option<
      T>&& /*unused*/) // NOLINT(cppcoreguidelines-rvalue-reference-param-not-moved)
    -> tenzir::None {
    return tenzir::None{};
  }
};

// -- CAF inspection -----------------------------------------------------------

template <class T>
  requires(not std::is_reference_v<T>)
struct caf::optional_inspector_traits<tenzir::Option<T>>
  : caf::optional_inspector_traits_base {
  using container_type = tenzir::Option<T>;
  using value_type = T;

  template <class... Ts>
  static void emplace(container_type& container, Ts&&... xs) {
    container.emplace(std::forward<Ts>(xs)...);
  }
};

template <class T>
  requires(not std::is_reference_v<T>)
struct caf::inspector_access<tenzir::Option<T>>
  : caf::optional_inspector_access<tenzir::Option<T>> {
  // nop
};

// -- fmt::formatter -----------------------------------------------------------

template <class T>
  requires(not std::is_reference_v<T>)
struct fmt::formatter<tenzir::Option<T>> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(tenzir::Option<T> const& x, FormatContext& ctx) const {
    if (x.is_some()) {
      return fmt::format_to(ctx.out(), "Some({})", *x);
    }
    return fmt::format_to(ctx.out(), "None");
  }
};
