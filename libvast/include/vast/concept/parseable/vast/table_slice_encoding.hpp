//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/concept/parseable/core/literal.hpp"
#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/string/char.hpp"

namespace vast {

struct table_slice_encoding_parser : parser_base<table_slice_encoding_parser> {
  using attribute = table_slice_encoding;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    using namespace parser_literals;
    // clang-format off
    auto p = "arrow"_p ->* [] { return table_slice_encoding::arrow; };
    // clang-format on
    return p(f, l, a);
  }
};

template <>
struct parser_registry<table_slice_encoding> {
  using type = table_slice_encoding_parser;
};

namespace parsers {

static auto const table_slice_encoding = table_slice_encoding_parser{};

} // namespace parsers
} // namespace vast
