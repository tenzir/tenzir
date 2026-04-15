//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/print.hpp"
#include "tenzir/error.hpp"

#include <caf/expected.hpp>

#include <string>
#include <type_traits>

namespace tenzir {

template <class To, registered_printer From, class... Opts>
auto to(From&& from, Opts&&... opts) -> caf::expected<std::string>
  requires(std::is_same_v<std::string, To>)
{
  std::string str;
  if (not print(std::back_inserter(str), from, std::forward<Opts>(opts)...)) {
    return caf::make_error(ec::print_error);
  }
  return str;
}

} // namespace tenzir
