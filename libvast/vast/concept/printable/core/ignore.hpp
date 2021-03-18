// SPDX-FileCopyrightText: (c) 2017 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <type_traits>

#include "vast/concept/printable/core/printer.hpp"

namespace vast {

/// Wraps a printer and ignores its attribute.
template <class Printer>
class ignore_printer : public printer<ignore_printer<Printer>> {
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

template <class Printer>
auto ignore(Printer&& p)
-> std::enable_if_t<
     is_printer_v<std::decay_t<Printer>>,
     ignore_printer<std::decay_t<Printer>>
   > {
  return ignore_printer<std::decay_t<Printer>>{std::forward<Printer>(p)};
}

} // namespace vast

