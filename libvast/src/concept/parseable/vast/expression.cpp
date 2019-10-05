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

#include "vast/concept/parseable/vast/expression.hpp"

#include "vast/data.hpp"          // for data, data_to_type
#include "vast/detail/string.hpp" // for join

#include <caf/atom.hpp>     // for atom_from_string
#include <caf/sum_type.hpp> // for visit

#include <type_traits> // for decay_t

namespace vast {

predicate predicate_parser::to_predicate(predicate_tuple xs) {
  return {std::move(std::get<0>(xs)), std::get<1>(xs),
          std::move(std::get<2>(xs))};
}

predicate::operand
predicate_parser::to_key_extractor(std::vector<std::string> xs) {
  // TODO: rather than doing all the work with the attributes, it would be nice
  // if the parser framework would just give us an iterator range over the raw
  // input. Then we wouldn't have to use expensive attributes and could simply
  // wrap a parser P into raw(P) or raw_str(P) to obtain a range/string_view.
  auto key = detail::join(xs, ".");
  return key_extractor{std::move(key)};
}

predicate::operand predicate_parser::to_attr_extractor(std::string x) {
  return attribute_extractor{caf::atom_from_string(x)};
}

predicate::operand predicate_parser::to_type_extractor(type x) {
  return type_extractor{std::move(x)};
}

predicate::operand predicate_parser::to_data_operand(data x) {
  return x;
}

predicate predicate_parser::to_data_predicate(data x) {
  auto infer_type = [](auto& d) -> type {
    return data_to_type<std::decay_t<decltype(d)>>{};
  };
  auto lhs = type_extractor{caf::visit(infer_type, x)};
  auto rhs = predicate::operand{std::move(x)};
  return {std::move(lhs), equal, std::move(rhs)};
}

} // namespace vast
