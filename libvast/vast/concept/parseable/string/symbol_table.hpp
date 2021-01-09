/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/detail/flat_map.hpp"

#include <algorithm>
#include <iterator>
#include <string>
#include <utility>

namespace vast {

/// A dynamic parser which acts as an associative array.
template <class T>
struct symbol_table : parser<symbol_table<T>> {
  using attribute = T;

  symbol_table() = default;

  explicit symbol_table(std::initializer_list<std::pair<std::string, T>> init)
    : symbols(init) {
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    // Do a O(n) search over the symbol table and keep track of the longest
    // match. This is a poorman's version of a ternary search trie.
    auto match = symbols.end();
    auto max_len = size_t{0};
    for (auto i = symbols.begin(); i != symbols.end(); ++i) {
      auto& [k, v] = *i;
      if (std::mismatch(k.begin(), k.end(), f, l).first == k.end()) {
        if (k.size() > max_len) {
          match = i;
          max_len = k.size();
        }
      }
    }
    if (match == symbols.end())
      return false;
    a = match->second;
    std::advance(f, max_len);
    return true;
  }

  detail::flat_map<std::string, T> symbols;
};

} // namespace vast
