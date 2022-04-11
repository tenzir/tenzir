//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/pipeline.hpp"

namespace vast {

struct pipeline_operator_parser : parser_base<pipeline_operator_parser> {
  using attribute = pipeline_operator;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, pipeline_operator& x) const;
};

template <>
struct parser_registry<pipeline_operator> {
  using type = pipeline_operator_parser;
};

namespace parsers {

constexpr auto pipeline_op = make_parser<vast::pipeline_operator>();

} // namespace parsers

struct pipeline_parser : parser_base<pipeline_parser> {
  using attribute = pipeline;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, pipeline& x) const;
};

template <>
struct parser_registry<pipeline> {
  using type = pipeline_parser;
};

namespace parsers {

constexpr auto pipeline = make_parser<vast::pipeline>();

} // namespace parsers
} // namespace vast
