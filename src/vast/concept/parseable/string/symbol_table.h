#ifndef VAST_CONCEPT_PARSEABLE_STRING_SYMBOL_TABLE_H
#define VAST_CONCEPT_PARSEABLE_STRING_SYMBOL_TABLE_H

#include "vast/util/radix_tree.h"

#include "vast/concept/parseable/core/parser.h"

namespace vast {

/// A dynamic parser which acts as an associative array. For symbols sharing
/// the same prefix, the parser returns the longest match.
template <typename T>
struct symbol_table : parser<symbol_table<T>> {
  using attribute = T;

  symbol_table() = default;

  symbol_table(std::initializer_list<std::pair<const std::string, T>> init)
    : symbols(init) {
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    auto input = std::string{f, l};
    auto prefixes = symbols.prefix_of(input);
    if (prefixes.empty())
      return false;
    auto longest_match = std::max_element(
      prefixes.begin(),
      prefixes.end(),
      [](auto x, auto y) { return x->first.size() < y->first.size(); }
    );
    a = (*longest_match)->second;
    std::advance(f, (*longest_match)->first.size());
    return true;
  }

  util::radix_tree<T> symbols;
};

} // namespace vast

#endif
