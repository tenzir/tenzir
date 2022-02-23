//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/numeric/integral.hpp"
#include "vast/concept/printable/print.hpp"
#include "vast/data/integer.hpp"

namespace vast {

struct integer_printer : printer_base<integer_printer> {
  using attribute = integer;

  template <class Iterator>
  bool print(Iterator& out, const integer& x) const {
    return printers::integral<int64_t, policy::force_sign>(out, x.value);
  }
};

template <>
struct printer_registry<integer> {
  using type = integer_printer;
};

namespace printers {
auto const integer = integer_printer{};
} // namespace printers

} // namespace vast
