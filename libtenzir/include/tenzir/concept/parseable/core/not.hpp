//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core/parser.hpp"

namespace tenzir {

template <class Parser>
class not_parser : public parser_base<not_parser<Parser>> {
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

} // namespace tenzir
