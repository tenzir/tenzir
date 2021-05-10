//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/detail/container.hpp"

#include <vector>

namespace vast {

template <class Lhs, class Rhs>
class list_parser : public parser<list_parser<Lhs, Rhs>> {
public:
  using lhs_attribute = typename Lhs::attribute;
  using rhs_attribute = typename Rhs::attribute;
  using container = detail::container<lhs_attribute>;
  using attribute = typename container::attribute;

  list_parser(Lhs lhs, Rhs rhs) : lhs_{std::move(lhs)}, rhs_{std::move(rhs)} {
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    if (!container::parse(lhs_, f, l, a))
      return false;
    auto save = f;
    while (rhs_(f, l, unused) && container::parse(lhs_, f, l, a))
      save = f;
    f = save;
    return true;
  }

private:
  Lhs lhs_;
  Rhs rhs_;
};

} // namespace vast
