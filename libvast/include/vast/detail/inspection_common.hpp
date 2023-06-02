//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/string_view.hpp>

#include <string_view>
#include <type_traits>
#include <utility>

namespace vast::detail {

/// A common class used by VAST inspectors as return type for the object method.
/// The CAF inspector API requires inspectors to have an object method.
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
/// inspection succeeds then it should proceed to inspect x.value. If all
/// provided fields succeed then the callback `cb` should be called.
template <class Inspector, class AfterInspectionCallback = decltype([] {
                             return true;
                           })>
class inspection_object {
public:
  explicit inspection_object(Inspector& inspector) : inspector_{inspector} {
  }

  template <class... Fields>
  constexpr bool fields(Fields&&... fs) {
    return (fs(inspector_) && ...) && callback_();
  }

  constexpr inspection_object& pretty_name(caf::string_view) noexcept {
    return *this;
  }

  template <class Callback>
  constexpr decltype(auto) on_load(Callback&& callback) {
    if constexpr (Inspector::is_loading) {
      return inspection_object<Inspector, std::remove_cvref_t<Callback>>{
        inspector_, std::forward<Callback>(callback)};
    } else {
      return *this;
    }
  }

  template <class Callback>
  constexpr decltype(auto) on_save(Callback&& callback) {
    if constexpr (!Inspector::is_loading) {
      return inspection_object<Inspector, std::remove_cvref_t<Callback>>{
        inspector_, std::forward<Callback>(callback)};
    } else {
      return *this;
    }
  }

private:
  // Allow on_load and on_save to construct inspection objects with custom
  // callbacks
  template <class I, class Callback>
  friend class inspection_object;

  inspection_object(Inspector& inspector, AfterInspectionCallback callback)
    : inspector_{inspector}, callback_{std::move(callback)} {
  }

  Inspector& inspector_;
  AfterInspectionCallback callback_;
};

template <class T>
class inspection_field {
public:
  explicit inspection_field(T& value) : value_{value} {
  }

  template <class Inspector>
  constexpr bool operator()(Inspector& inspector) {
    return inspector.apply(value_);
  }

private:
  T& value_;
};

template <class Inspector, class... Args>
auto apply_all(Inspector& f, Args&&... args) {
  return (f.apply(std::forward<Args>(args)) && ...);
}

template <class Inspector, class Enum>
  requires std::is_enum_v<Enum>
bool inspect_enum(Inspector& f, Enum& x) {
  using underlying_type = std::underlying_type_t<Enum>;
  if constexpr (Inspector::is_loading) {
    underlying_type tmp;
    if (!f.apply(tmp))
      return false;
    x = static_cast<Enum>(tmp);
    return true;
  } else {
    return f.apply(static_cast<underlying_type>(x));
  }
}

} // namespace vast::detail
