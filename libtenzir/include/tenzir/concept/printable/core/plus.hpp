//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/core/printer.hpp"
#include "tenzir/concept/support/detail/attr_fold.hpp"

#include <vector>

namespace tenzir {

template <class Printer>
class plus_printer : public printer_base<plus_printer<Printer>> {
public:
  using inner_attribute = typename Printer::attribute;
  using attribute = detail::attr_fold_t<std::vector<inner_attribute>>;

  explicit plus_printer(Printer p) : printer_{std::move(p)} {
  }

  template <class Iterator, class Attribute>
  bool print(Iterator& out, const Attribute& a) const {
    using std::begin;
    using std::end;
    auto f = begin(a);
    auto l = end(a);
    if (f == l)
      return false;
    do {
      if (!printer_.print(out, *f++))
        return false;
    } while (f != l);
    return true;
  }

private:
  Printer printer_;
};

} // namespace tenzir
