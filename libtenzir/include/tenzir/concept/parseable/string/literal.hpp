//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core/parser.hpp"

#include <string_view>

namespace tenzir {

class literal_parser : public parser_base<literal_parser> {
public:
  using attribute = std::string_view;

  constexpr literal_parser(std::string_view str) : str_{str} {
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    auto i = f;
    auto begin = str_.begin();
    auto end = str_.end();
    while (begin != end) {
      if (i == l or *i++ != *begin++) {
        return false;
      }
    }
    f = i;
    return true;
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    if (not parse(f, l, unused)) {
      return false;
    }
    x = Attribute{str_.data(), str_.size()};
    return true;
  }

private:
  std::string_view str_;
};

namespace parsers {

using lit = literal_parser;

} // namespace parsers
} // namespace tenzir
