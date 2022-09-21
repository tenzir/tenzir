//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <string_view>
#include <utility>

namespace vast::detail {

template <class Inspector, class AfterInspectionCallback>
class inspection_object_with_after_inspection_callback;

/// Common class used by VAST inspectors as a return type for object method.
/// The CAF inspector API requires an inspector to have object method.
/// This method should return an object which guides the inspection procedure.
/// Example of usage:
/// bool inspect(Inspector& f, some_type& x)
///{
///   auto cb = []{};
///   return f.object(x).
///     on_load(cb).
///     fields(f.field("name", x.name), f.field("value", x.value));
///}
/// Such code should result in inspection of x.name by the inspector. If the
/// inspection succeeds then it should proceed to inspection of x.value. If all
/// provided fields succeed then callback (cb variable) should be called. The
/// provided string literals are optionally used by human readable CAF
/// inspectors. Current VAST inspectors have no usage for them.
template <class Inspector>
class inspection_object {
public:
  explicit inspection_object(Inspector& inspector) : inspector_{inspector} {
  }

  template <class... Fields>
  constexpr bool fields(Fields&&... fields) {
    return (fields(inspector_) && ...);
  }

  constexpr inspection_object& pretty_name(std::string_view) noexcept {
    return *this;
  }

  template <class Callback>
    requires(Inspector::is_loading == true)
  constexpr decltype(auto) on_load(Callback callback) {
    return inspection_object_with_after_inspection_callback{
      inspector_, std::move(callback)};
  }

  template <class Callback>
    requires(Inspector::is_loading == false)
  constexpr decltype(auto) on_save(Callback callback) {
    return inspection_object_with_after_inspection_callback{
      inspector_, std::move(callback)};
  }

private:
  Inspector& inspector_;
};

/// Enhanced inspection_object that is responsible for calling callback after
/// inspection.
template <class Inspector, class AfterInspectionCallback>
class inspection_object_with_after_inspection_callback {
public:
  inspection_object_with_after_inspection_callback(
    Inspector& inspector, AfterInspectionCallback callback)
    : inspector_{inspector}, callback_{std::move(callback)} {
  }

  template <class... Fields>
  constexpr bool fields(Fields&&... fields) {
    return (fields(inspector_) && ...) && callback_();
  }

  constexpr inspection_object_with_after_inspection_callback&
  pretty_name(std::string_view) noexcept {
    return *this;
  }

private:
  Inspector& inspector_;
  AfterInspectionCallback callback_;
};

template <class T>
class inspection_field {
public:
  explicit inspection_field(T& value) : value_{value} {
  }

  template <class Inspector>
  constexpr bool operator()(Inspector& inspector) noexcept(
    std::declval<Inspector>().apply(std::declval<T&>())) {
    return inspector.apply(value_);
  }

private:
  T& value_;
};

} // namespace vast::detail
