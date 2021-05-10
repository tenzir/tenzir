//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/print.hpp"

#include <string>
#include <type_traits>

namespace vast {

template <class From, class... Opts>
auto to_string(From&& from, Opts&&... opts)
  -> std::enable_if_t<
       is_printable_v<
          std::back_insert_iterator<std::string>, std::decay_t<From>
        >,
       std::string
     > {
  std::string str;
  print(std::back_inserter(str), from, std::forward<Opts>(opts)...);
  return str;
}

} // namespace vast
