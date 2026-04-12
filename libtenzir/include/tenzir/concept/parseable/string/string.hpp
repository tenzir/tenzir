//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core/parser.hpp"

#include <string>

namespace tenzir {

class string_parser : public parser_base<string_parser> {
public:
  using attribute = std::string;

  string_parser(std::string str) : str_{std::move(str)} {
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    auto i = f;
    auto begin = str_.begin();
    auto end = str_.end();
    while (begin != end)
      if (i == l or *i++ != *begin++)
        return false;
    f = i;
    return true;
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    a.clear();
    auto out = std::back_inserter(a);
    auto i = f;
    auto begin = str_.begin();
    auto end = str_.end();
    while (begin != end)
      if (i == l or *i != *begin++)
        return false;
      else
        *out++ = *i++;
    f = i;
    return true;
  }

private:
  std::string str_;
};

namespace parsers {

using str = string_parser;

} // namespace parsers
} // namespace tenzir
