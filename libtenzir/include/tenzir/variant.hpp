//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/debug_writer.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/error.hpp"
#include "tenzir/variant_traits.hpp"

#include <caf/detail/pretty_type_name.hpp>
#include <fmt/format.h>

#include <variant>

namespace tenzir {

namespace detail {

template <class Result, class F>
auto make_conversion_wrapper(F f) -> auto {
  return [f = std::move(f)]<class... Args>(Args&&... args) -> Result {
    static_assert(std::invocable<F, Args...>);
    if constexpr (std::same_as<std::invoke_result_t<F, Args...>, void>) {
      // TODO: We assume that the function is effectively `[[noreturn]]`.
      TENZIR_UNREACHABLE();
    } else {
      return Result{std::invoke(f, std::forward<Args>(args)...)};
    }
  };
}

template <class Result, class Variant, class... Fs>
auto match(Variant&& variant, Fs&&... fs) -> decltype(auto) {
  if constexpr (std::same_as<Result, void>) {
    return std::visit(detail::overload{std::forward<Fs>(fs)...},
                      std::forward<Variant>(variant));
  } else {
    return std::visit(make_conversion_wrapper<Result>(
                        detail::overload{std::forward<Fs>(fs)...}),
                      std::forward<Variant>(variant));
  }
}

} // namespace detail

/// A variant type with a different `inspect()` implementation than
/// `std::variant`.
template <class... Ts>
class variant : public std::variant<Ts...> {
public:
  using std::variant<Ts...>::variant;

  template <class T>
  static constexpr auto can_have = (std::same_as<T, Ts> || ...);

  template <class Inspector>
  friend auto inspect(Inspector& f, variant& x) -> bool {
    if (auto dbg = as_debug_writer(f)) {
      return x.match([&]<class T>(const T& y) {
        auto name = std::string{};
        if constexpr (std::same_as<T, std::string>) {
          name = "string";
        } else if constexpr (std::same_as<T, std::int64_t>) {
          name = "int64";
        } else if constexpr (std::same_as<T, std::uint64_t>) {
          name = "uint64";
        } else {
          name = caf::detail::pretty_type_name(typeid(T));
          auto index = name.find_last_of('.');
          if (index != std::string::npos) {
            name = name.substr(index + 1);
          }
        }
        return dbg->prepend("{} ", name) && dbg->apply(y);
      });
    }
    // Unlike `caf::inspector_access<std::variant<Ts...>>::apply(f, x)`, this
    // implementation does not need a CAF type-id, and it also works for
    // `caf::json_writer`. We use index-based serialization if the inspector
    // does not represent a human-readable format, and type names otherwise.
    if constexpr (Inspector::is_loading) {
      if (!f.has_human_readable_format()) {
        auto index = size_t{};
        if (!f.apply(index)) {
          return false;
        }
        if (index >= sizeof...(Ts)) {
          f.set_error(caf::make_error(ec::serialization_error,
                                      fmt::format("variant index {} too big "
                                                  "for variant of {}",
                                                  index, sizeof...(Ts))));
          return false;
        }
        auto emplace_idx = [&]<size_t I>() {
          if (index != I) {
            return false;
          }
          auto& y = x.template emplace<I>();
          return f.apply(y);
        };
        auto emplace = [&]<size_t... Is>(std::index_sequence<Is...>) {
          return (emplace_idx.template operator()<Is>() || ...);
        };
        return emplace(std::index_sequence_for<Ts...>());
      }
      auto type_name = std::string{};
      auto count = size_t{};
      if (!f.begin_associative_array(count)) {
        return false;
      }
      if (count != 1) {
        f.set_error(caf::make_error(ec::serialization_error,
                                    fmt::format("incorrect variant associative "
                                                "array count of {}",
                                                count)));
        return false;
      }
      if (!(f.begin_key_value_pair() && f.value(type_name))) {
        return false;
      }
      auto success = false;
      auto check = [&]<size_t I>() -> bool {
        using type = std::variant_alternative_t<I, std::variant<Ts...>>;
        if (caf::detail::pretty_type_name(typeid(type)) != type_name) {
          return false;
        }
        auto& y = x.template emplace<I>();
        success = f.apply(y);
        return true;
      };
      auto check_all = [&]<size_t... Is>(std::index_sequence<Is...>) {
        auto found = (check.template operator()<Is>() || ...);
        if (!found) {
          f.set_error(caf::make_error(
            ec::serialization_error,
            fmt::format("could not resolve type name `{}`", type_name)));
        }
      };
      check_all(std::index_sequence_for<Ts...>());
      return success && f.end_key_value_pair() && f.end_associative_array();
    } else {
      return std::visit(
        [&](auto& y) {
          if (!f.has_human_readable_format()) {
            return f.apply(x.index()) && f.apply(y);
          }
          return f.begin_associative_array(1) && f.begin_key_value_pair()
                 && f.value(caf::detail::pretty_type_name(typeid(y)))
                 && f.apply(y) && f.end_key_value_pair()
                 && f.end_associative_array();
        },
        x);
    }
  }

  template <class Result = void, class... Fs>
  auto match(Fs&&... fs) & -> decltype(auto) {
    return detail::match<Result>(*this, std::forward<Fs>(fs)...);
  }

  template <class Result = void, class... Fs>
  auto match(Fs&&... fs) const& -> decltype(auto) {
    return detail::match<Result>(*this, std::forward<Fs>(fs)...);
  }

  template <class Result = void, class... Fs>
  auto match(Fs&&... fs) && -> decltype(auto) {
    return detail::match<Result>(std::move(*this), std::forward<Fs>(fs)...);
  }

  template <class Result = void, class... Fs>
  auto match(Fs&&... fs) const&& -> decltype(auto) {
    return detail::match<Result>(std::move(*this), std::forward<Fs>(fs)...);
  }
};

template <class... Ts>
class variant_traits<variant<Ts...>> {
public:
  static constexpr auto count = sizeof...(Ts);

  static constexpr auto index(const std::variant<Ts...>& x) -> size_t {
    return x.index();
  }

  template <size_t I>
  static constexpr auto get(const std::variant<Ts...>& x) -> decltype(auto) {
    return *std::get_if<I>(&x);
  }
};

} // namespace tenzir
