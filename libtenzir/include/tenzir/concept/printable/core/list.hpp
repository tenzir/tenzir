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

#include <iterator>
#include <vector>

namespace tenzir {

template <class Lhs, class Rhs>
class list_printer : public printer_base<list_printer<Lhs, Rhs>> {
public:
  using lhs_attribute = typename Lhs::attribute;
  using rhs_attribute = typename Rhs::attribute;
  using attribute = detail::attr_fold_t<std::vector<lhs_attribute>>;

  list_printer(Lhs lhs, Rhs rhs) : lhs_{std::move(lhs)}, rhs_{std::move(rhs)} {
    // nop
  }

  template <class Iterator, class Attribute>
  bool print(Iterator& out, const Attribute& a) const {
    using std::begin;
    using std::end;
    auto f = begin(a);
    auto l = end(a);
    if (f == l)
      return true;
    if (!lhs_.print(out, *f))
      return false;
    for (++f; f != l; ++f)
      if (!(rhs_.print(out, unused) && lhs_.print(out, *f)))
        return false;
    return true;
  }

private:
  Lhs lhs_;
  Rhs rhs_;
};

} // namespace tenzir
