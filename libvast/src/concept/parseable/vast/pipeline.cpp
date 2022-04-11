
//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/concept/parseable/vast/pipeline.hpp"

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/string.hpp"
#include "vast/concept/parseable/vast/expression.hpp"

namespace vast {

template <class Iterator>
bool pipeline_operator_parser::parse(Iterator& f, const Iterator& l,
                                     pipeline_operator& x) const {
  using namespace parsers;
  auto op = +(parsers::printable - space - '|');
  auto parser = ignore(*space) >> (op % +space);
  return parser(f, l, x.xs);
}

template bool pipeline_operator_parser::parse(std::string::iterator&,
                                              const std::string::iterator&,
                                              pipeline_operator&) const;
template bool
pipeline_operator_parser::parse(std::string::const_iterator&,
                                const std::string::const_iterator&,
                                pipeline_operator&) const;
template bool pipeline_operator_parser::parse(char const*&, char const* const&,
                                              pipeline_operator&) const;

template <class Iterator>
bool pipeline_parser::parse(Iterator& f, const Iterator& l, pipeline& x) const {
  using namespace parsers;
  using namespace parser_literals;
  auto ws = ignore(*space);
  auto pipe = ws >> "|"_p >> ws;
  auto parser = expr >> *(pipe >> pipeline_op);
  return parser(f, l, x.root, x.operators);
}

template bool
pipeline_parser::parse(std::string::iterator&, const std::string::iterator&,
                       pipeline&) const;
template bool
pipeline_parser::parse(std::string::const_iterator&,
                       const std::string::const_iterator&, pipeline&) const;
template bool
pipeline_parser::parse(char const*&, char const* const&, pipeline&) const;

} // namespace vast
