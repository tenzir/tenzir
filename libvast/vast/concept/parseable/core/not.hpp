// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core/parser.hpp"

namespace vast {

template <class Parser>
class not_parser : public parser<not_parser<Parser>> {
public:
  using attribute = unused_type;

  constexpr explicit not_parser(Parser p) : parser_{std::move(p)} {
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute&) const {
    auto i = f; // Do not consume input.
    return !parser_(i, l, unused);
  }

private:
  Parser parser_;
};

} // namespace vast

