//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/prune.hpp"

namespace tenzir {

struct pruner {
  expression operator()(caf::none_t) const {
    return expression{};
  }
  expression operator()(const conjunction& c) const {
    return conjunction{run(c)};
  }
  expression operator()(const disjunction& d) const {
    return disjunction{run(d)};
  }
  expression operator()(const negation& n) const {
    return negation{match(n.expr(), *this)};
  }
  expression operator()(const predicate& p) const {
    return p;
  }

  [[nodiscard]] std::vector<expression>
  run(const std::vector<expression>& connective) const {
    std::vector<expression> result;
    result.reserve(connective.size());
    std::vector<tenzir::predicate*> memo;
    for (const auto& operand : connective) {
      bool optimized = false;
      if (const auto* pred = try_as<predicate>(&operand)) {
        if (is<field_extractor>(pred->lhs)
            || (is<type_extractor>(pred->lhs)
                && as<type_extractor>(pred->lhs).type == string_type{})) {
          if (const auto* d = try_as<data>(&pred->rhs)) {
            if (is<std::string>(*d)) {
              auto const* fe = try_as<field_extractor>(&pred->lhs);
              if (!fe || !unprunable_fields_.contains(fe->field)) {
                optimized = true;
                // Replace the concrete field name by `:string` if this is
                // the second time we lookup this value.
                if (auto it = std::find_if(memo.begin(), memo.end(),
                                           [&](const predicate* p) {
                                             return p->op == pred->op
                                                    && p->rhs == pred->rhs;
                                           });
                    it != memo.end()) {
                  auto& p = *it;
                  p->lhs = tenzir::type_extractor{
                    tenzir::type{tenzir::string_type{}}};
                  continue;
                } else {
                  result.push_back(operand);
                  memo.push_back(try_as<predicate>(&result.back()));
                }
              }
            }
          }
        }
      }
      if (!optimized)
        result.push_back(match(operand, *this));
    }
    return result;
  }

  detail::heterogeneous_string_hashset const& unprunable_fields_;
};

// Runs the `pruner` and `hoister` until the input is unchanged.
expression prune(expression e, const detail::heterogeneous_string_hashset& hs) {
  expression result = match(e, pruner{hs});
  while (result != e) {
    std::swap(result, e);
    result = hoist(match(e, pruner{hs}));
  }
  return result;
}

} // namespace tenzir
