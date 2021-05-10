//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/string.hpp"
#include "vast/error.hpp"

namespace vast {

struct error_printer : printer<error_printer> {
  using attribute = caf::error;

  template <class Iterator>
  bool print(Iterator& out, const caf::error& e) const {
    auto msg = to_string(e);
    return printers::str.print(out, msg);
  }
};

template <>
struct printer_registry<caf::error> {
  using type = error_printer;
};

} // namespace vast
