//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/panic.hpp"
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

namespace detail {

/// Storage backend for `Option<T>` when `T` is a value type.
template <class T>
class OptionStorage {
public:
  OptionStorage() noexcept = default;
  explicit OptionStorage(T value) noexcept(
    std::is_nothrow_move_constructible_v<T>)
    : inner_{std::move(value)} {
  }
  explicit OptionStorage(std::optional<T> opt) noexcept(
    std::is_nothrow_move_constructible_v<std::optional<T>>)
    : inner_{std::move(opt)} {
  }
  auto is_some() const noexcept -> bool {
    return inner_.has_value();
  }
  template <class Self>
  // NOLINTNEXTLINE(cppcoreguidelines-missing-std-forward)
  auto get(this Self&& self) -> decltype(auto) {
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
  template <class Self>
  // NOLINTNEXTLINE(cppcoreguidelines-missing-std-forward)
  auto get(this Self&& self) -> decltype(auto) {
    return std::forward_like<Self>(*self.ptr_);
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
  explicit(false) Option(None) noexcept {
  }

  /// Constructs from a value (non-reference `T` only).
  template <class U = T>
    requires(not std::is_reference_v<T> and std::constructible_from<T, U>
             and not std::same_as<std::remove_cvref_t<U>, None>
             and not std::same_as<std::remove_cvref_t<U>, Option>
             and not std::same_as<std::remove_cvref_t<U>, std::optional<T>>)
  explicit(not std::convertible_to<U, T>)
    Option(U&& value) noexcept(std::is_nothrow_constructible_v<T, U&&>
                               and std::is_nothrow_constructible_v<Storage, T>)
    : storage_{T{std::forward<U>(value)}} {
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
    requires(not std::is_reference_v<T> and std::convertible_to<U, T>)
  auto operator=(U&& value) -> Option& {
    storage_.emplace(std::forward<U>(value));
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
  explicit operator bool() const noexcept {
    return is_some();
  }

  /// Returns whether the option contains a value.
  auto is_some() const noexcept -> bool {
    return storage_.is_some();
  }

  /// Returns whether the option is empty.
  auto is_none() const noexcept -> bool {
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
  auto operator*(this Self&& self) -> decltype(auto) {
    if (not self.is_some()) [[unlikely]] {
      panic("called Option::operator* on a None value");
    }
    return std::forward<Self>(self).storage_.get();
  }

  /// Pointer-style access (non-reference `T` only). Panics if empty.
  auto operator->() -> Value*
    requires(not std::is_reference_v<T>)
  {
    if (not is_some()) [[unlikely]] {
      panic("called Option::operator-> on a None value");
    }
    return &storage_.get();
  }

  /// Pointer-style access (non-reference `T` only). Panics if empty.
  auto operator->() const -> Value const*
    requires(not std::is_reference_v<T>)
  {
    if (not is_some()) [[unlikely]] {
      panic("called Option::operator-> on a None value");
    }
    return &storage_.get();
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
    requires requires(Value const& a, std::remove_reference_t<U> const& b) {
      { a == b } -> std::convertible_to<bool>;
    }
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

  /// An option equals a value iff it is Some and the values are equal.
  template <class U>
    requires(not std::same_as<std::remove_cvref_t<U>, None>
             and not std::same_as<std::remove_cvref_t<U>, Option>
             and requires(Value const& a, U const& b) {
               { a == b } -> std::convertible_to<bool>;
             })
  friend auto operator==(Option const& lhs, U const& rhs) -> bool {
    return lhs.is_some() and *lhs == rhs;
  }

  /// Orders two options. None is less than any Some.
  template <class U>
    requires requires(Value const& a, std::remove_reference_t<U> const& b) {
      { a <=> b };
    }
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
             and not std::same_as<std::remove_cvref_t<U>, Option>
             and requires(Value const& a, U const& b) {
               { a <=> b };
             })
  friend auto operator<=>(Option const& lhs, U const& rhs) {
    if (lhs.is_some()) {
      return *lhs <=> rhs;
    }
    return std::strong_ordering::less;
  }

private:
  template <class>
  friend class Option;
  template <concepts::unqualified>
  friend class variant_traits;

  Storage storage_;
};

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
