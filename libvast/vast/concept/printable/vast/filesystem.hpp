// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/string.hpp"
#include "vast/path.hpp"

namespace vast {

struct path_printer : printer<path_printer> {
  using attribute = path;

  template <class Iterator>
  bool print(Iterator& out, const path& p) const {
    return printers::str.print(out, p.str());
  }
};

template <>
struct printer_registry<path> {
  using type = path_printer;
};

} // namespace vast

