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

#include <caf/none.hpp>

namespace vast {

struct none_printer : printer_base<none_printer> {
  using attribute = caf::none_t;

  template <class Iterator>
  bool print(Iterator& out, caf::none_t) const {
    return printers::str.print(out, "null");
  }
};

template <>
struct printer_registry<caf::none_t> {
  using type = none_printer;
};

} // namespace vast
