//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2017 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core/parser.hpp"

namespace tenzir {

class epsilon_parser : public parser_base<epsilon_parser> {
public:
  using attribute = unused_type;

  template <class Iterator, class Attribute>
  bool parse(Iterator&, const Iterator&, Attribute&) const {
    return true;
  }
};

namespace parsers {

auto const eps = epsilon_parser{};

} // namespace parsers
} // namespace tenzir
