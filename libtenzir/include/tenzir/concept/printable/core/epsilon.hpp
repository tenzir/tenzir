//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/core/printer.hpp"

namespace tenzir {

class epsilon_printer : public printer_base<epsilon_printer> {
public:
  using attribute = unused_type;

  template <class Iterator>
  bool print(Iterator&, unused_type) const {
    return true;
  }
};

namespace printers {

auto const eps = epsilon_printer{};

} // namespace printers
} // namespace tenzir
