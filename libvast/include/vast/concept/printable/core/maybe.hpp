//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/core/printer.hpp"

namespace vast {

/// Like ::optional_printer, but exposes `T` instead of `optional<T>` as
/// attribute.
template <class Printer>
class maybe_printer : public printer_base<maybe_printer<Printer>> {
public:
  using attribute = typename Printer::attribute;

  explicit maybe_printer(Printer p)
    : printer_{std::move(p)} {
  }

  template <class Iterator, class Attribute>
  bool print(Iterator& out, const Attribute& a) const {
    printer_.print(out, a);
    return true;
  }

private:
  Printer printer_;
};

} // namespace vast
