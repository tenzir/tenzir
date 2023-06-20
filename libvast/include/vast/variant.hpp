//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/overload.hpp"
#include "vast/error.hpp"

#include <caf/detail/pretty_type_name.hpp>
#include <fmt/format.h>

#include <variant>

namespace vast {

/// A variant type with a different `inspect()` implementation than
/// `std::variant`.
template <class... Ts>
class variant : public std::variant<Ts...> {
public:
  using std::variant<Ts...>::variant;

  template <class Inspector>
  friend auto inspect(Inspector& f, variant& x) {
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

  template <class... Fs>
  auto match(Fs&&... fs) -> decltype(auto) {
    return std::visit(detail::overload{std::forward<Fs>(fs)...}, *this);
  }

  template <class... Fs>
  auto match(Fs&&... fs) const -> decltype(auto) {
    return std::visit(detail::overload{std::forward<Fs>(fs)...}, *this);
  }
};

} // namespace vast
