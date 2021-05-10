//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core/parser.hpp"

namespace vast {

/// A parser that ingores the next *n* bytes.
class skip_parser : public parser<skip_parser> {
public:
  using attribute = unused_type;

  explicit skip_parser(size_t n) : n_{n} {
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    if (f + n_ >= l)
      return false;
    f += n_;
    return true;
  }

private:
  size_t n_;
};

namespace parsers {

inline auto skip(size_t n) {
  return skip_parser{n};
}

} // namespace parsers
} // namespace vast
