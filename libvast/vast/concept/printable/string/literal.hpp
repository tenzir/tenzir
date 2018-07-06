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

#include <string>
#include <type_traits>

#include "vast/concept/printable/string/string.hpp"

namespace vast {

class literal_printer : public printer<literal_printer> {
  template <class T>
  using enable_if_non_fp_arithmetic =
    std::enable_if_t<std::is_arithmetic<T>{} && !std::is_floating_point<T>{}>;

  template <class T>
  using enable_if_fp = std::enable_if_t<std::is_floating_point<T>{}>;

public:
  using attribute = unused_type;

  literal_printer(bool b) : str_{b ? "T" : "F"} {
  }

  template <class T>
  literal_printer(T x, enable_if_non_fp_arithmetic<T>* = nullptr)
    : str_{std::to_string(x)} {
  }

  template <class T>
  literal_printer(T x, enable_if_fp<T>* = nullptr) : str_{std::to_string(x)} {
    // Remove trailing zeros.
    str_.erase(str_.find_last_not_of('0') + 1, std::string::npos);
  }

  literal_printer(char c) : str_{c} {
  }

  literal_printer(std::string str) : str_(std::move(str)) {
  }

  template <class Iterator>
  bool print(Iterator& out, unused_type) const {
    return printers::str.print(out, str_);
  }

private:
  std::string str_;
};

namespace printers {

using lit = literal_printer;

} // namespace printers
} // namespace vast

