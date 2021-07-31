//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/support/detail/attr_fold.hpp"

#include <vector>

namespace vast {

template <class Printer>
class kleene_printer : public printer_base<kleene_printer<Printer>> {
public:
  using inner_attribute = typename Printer::attribute;
  using attribute = detail::attr_fold_t<std::vector<inner_attribute>>;

  explicit kleene_printer(Printer p) : printer_{std::move(p)} {
  }

  template <class Iterator, class Attribute>
  bool print(Iterator& out, const Attribute& a) const {
    using std::begin;
    using std::end;
    auto f = begin(a);
    auto l = end(a);
    for (; f != l; ++f)
      if (!printer_.print(out, *f))
        return false;
    return true;
  }

private:
  Printer printer_;
};

} // namespace vast
