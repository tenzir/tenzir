//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core/parser.hpp"

namespace vast {

// The AND parser does not consume its input and serves as basic look-ahead.
template <class Parser>
class and_parser : public parser<and_parser<Parser>> {
public:
  using attribute = unused_type;

  constexpr explicit and_parser(Parser p) : parser_{std::move(p)} {
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute&) const {
    auto i = f; // Do not consume input.
    return parser_(i, l, unused);
  }

private:
  Parser parser_;
};

} // namespace vast
