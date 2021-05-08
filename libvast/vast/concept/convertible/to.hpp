//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/type_traits.hpp"
#include "vast/error.hpp"

#include <caf/expected.hpp>

#include <type_traits>

namespace vast {

/// Converts one type to another.
/// @tparam To The type to convert `From` to.
/// @tparam From The type to convert to `To`.
/// @param from The instance to convert.
/// @returns *from* converted to `T`.
template <class To, class From, class... Opts>
auto to(From&& from, Opts&&... opts)
  -> std::enable_if_t<std::is_convertible_v<std::decay_t<From>, To>,
                      caf::expected<To>> {
  using return_type
    = decltype(convert(from, std::declval<To&>(), std::forward<Opts>(opts)...));
  if constexpr (std::is_same_v<return_type, bool>) {
    caf::expected<To> result{To()};
    if (convert(from, *result, std::forward<Opts>(opts)...))
      return result;
    return caf::make_error(ec::convert_error);
  } else if constexpr (std::is_same_v<return_type, caf::error>) {
    To result;
    if (auto err = convert(from, result, std::forward<Opts>(opts)...))
      return err;
    return result;
  } else {
    static_assert(detail::always_false_v<return_type>, "invalid return type");
  }
}

template <class To, class From, class... Opts>
auto to_string(From&& from, Opts&&... opts) -> std::enable_if_t<
  std::conjunction_v<std::is_same<To, std::string>,
                     std::is_convertible<std::decay_t<From>, To>>,
  To> {
  std::string str;
  if (convert(from, str, std::forward<Opts>(opts)...))
    return str;
  return {}; // TODO: throw?
}

} // namespace vast
