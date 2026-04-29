//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/print.hpp"

#include <string>
#include <type_traits>

namespace tenzir {

template <class From, class... Opts>
auto to_string(From&& from, Opts&&... opts)
  requires(printable<std::back_insert_iterator<std::string>, std::decay_t<From>>)
{
  std::string str;
  print(std::back_inserter(str), from, std::forward<Opts>(opts)...);
  return str;
}

} // namespace tenzir
