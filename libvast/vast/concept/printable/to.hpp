/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

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

