//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/prune.hpp"

namespace vast {

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
    return negation{caf::visit(*this, n.expr())};
  }
  expression operator()(const predicate& p) const {
    return p;
  }

  [[nodiscard]] std::vector<expression>
  run(const std::vector<expression>& connective) const {
    std::vector<expression> result;
    result.reserve(connective.size());
    std::vector<vast::predicate*> memo;
    for (const auto& operand : connective) {
      bool optimized = false;
      if (const auto* pred = caf::get_if<predicate>(&operand)) {
        if (caf::holds_alternative<extractor>(pred->lhs)
            || (caf::holds_alternative<type_extractor>(pred->lhs)
                && caf::get<type_extractor>(pred->lhs).type == string_type{})) {
          if (const auto* d = caf::get_if<data>(&pred->rhs)) {
            if (caf::holds_alternative<std::string>(*d)) {
              auto const* fe = caf::get_if<extractor>(&pred->lhs);
              if (!fe || !unprunable_fields_.contains(fe->value)) {
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
                  p->lhs
                    = vast::type_extractor{vast::type{vast::string_type{}}};
                  continue;
                } else {
                  result.push_back(operand);
                  memo.push_back(caf::get_if<predicate>(&result.back()));
                }
              }
            }
          }
        }
      }
      if (!optimized)
        result.push_back(caf::visit(*this, operand));
    }
    return result;
  }

  detail::heterogenous_string_hashset const& unprunable_fields_;
};

// Runs the `pruner` and `hoister` until the input is unchanged.
expression prune(expression e, const detail::heterogenous_string_hashset& hs) {
  expression result = caf::visit(pruner{hs}, e);
  while (result != e) {
    std::swap(result, e);
    result = hoist(caf::visit(pruner{hs}, e));
  }
  return result;
}

} // namespace vast
