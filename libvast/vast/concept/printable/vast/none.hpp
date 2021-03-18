// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/none.hpp>

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/string.hpp"

namespace vast {

struct none_printer : printer<none_printer> {
  using attribute = caf::none_t;

  template <class Iterator>
  bool print(Iterator& out, caf::none_t) const {
    return printers::str.print(out, "nil");
  }
};

template <>
struct printer_registry<caf::none_t> {
  using type = none_printer;
};

} // namespace vast

