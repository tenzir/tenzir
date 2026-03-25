//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2017 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/core/printer.hpp"

#include <type_traits>

namespace tenzir {

/// Wraps a printer and ignores its attribute.
template <class Printer>
class ignore_printer : public printer_base<ignore_printer<Printer>> {
public:
  using attribute = unused_type;

  explicit ignore_printer(Printer p) : printer_{std::move(p)} {
  }

  template <class Iterator, class Attribute>
  bool print(Iterator& out, const Attribute&) const {
    return printer_.print(out, unused);
  }

private:
  Printer printer_;
};

template <printer Printer>
auto ignore(Printer&& p) -> ignore_printer<std::decay_t<Printer>> {
  return ignore_printer<std::decay_t<Printer>>{std::forward<Printer>(p)};
}

} // namespace tenzir
