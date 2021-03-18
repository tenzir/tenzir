// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <string>
#include <type_traits>

#include "vast/concept/printable/print.hpp"

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
