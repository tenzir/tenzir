//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/print.hpp"
#include "vast/error.hpp"

#include <caf/expected.hpp>

#include <string>
#include <type_traits>

namespace vast {

template <class To, class From, class... Opts>
auto to(From&& from, Opts&&... opts)
  -> std::enable_if_t<std::is_same<std::string, To>{}
                        && has_printer_v<std::decay_t<From>>,
                      caf::expected<std::string>> {
  std::string str;
  if (!print(std::back_inserter(str), from, std::forward<Opts>(opts)...))
    return caf::make_error(ec::print_error);
  return str;
}

} // namespace vast
