//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/expression_visitors.hpp"

#include "vast/fwd.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/concept/parseable/vast/type.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/operator.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/data.hpp"
#include "vast/detail/assert.hpp"
#include "vast/die.hpp"
#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/table_slice.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

#include <algorithm>
#include <regex>

namespace vast {

expression hoister::operator()(caf::none_t) const {
  return expression{};
}

expression hoister::operator()(const conjunction& c) const {
  conjunction hoisted;
  for (auto& op : c)
    if (auto inner = caf::get_if<conjunction>(&op))
      for (auto& inner_op : *inner)
        hoisted.push_back(caf::visit(*this, inner_op));
    else
      hoisted.push_back(caf::visit(*this, op));
  return hoisted.size() == 1 ? hoisted[0] : hoisted;
}

expression hoister::operator()(const disjunction& d) const {
  disjunction hoisted;
  for (auto& op : d)
    if (auto inner = caf::get_if<disjunction>(&op))
      for (auto& inner_op : *inner)
        hoisted.push_back(caf::visit(*this, inner_op));
    else
      hoisted.push_back(caf::visit(*this, op));
  return hoisted.size() == 1 ? hoisted[0] : hoisted;
}

expression hoister::operator()(const negation& n) const {
  return {negation{caf::visit(*this, n.expr())}};
}

expression hoister::operator()(const predicate& p) const {
  return {p};
}

expression aligner::operator()(caf::none_t) const {
  return caf::none;
}

expression aligner::operator()(const conjunction& c) const {
  conjunction result;
  for (auto& op : c)
    result.push_back(caf::visit(*this, op));
  return result;
}

expression aligner::operator()(const disjunction& d) const {
  disjunction result;
  for (auto& op : d)
    result.push_back(caf::visit(*this, op));
  return result;
}

expression aligner::operator()(const negation& n) const {
  return {negation{caf::visit(*this, n.expr())}};
}

expression aligner::operator()(const predicate& p) const {
  auto is_extractor
    = [](auto& operand) { return !caf::holds_alternative<data>(operand); };
  // Already aligned if LHS is an extractor or no extractor present.
  if (is_extractor(p.lhs) || !is_extractor(p.rhs))
    return p;
  return predicate{p.rhs, flip(p.op), p.lhs};
}

denegator::denegator(bool negate) : negate_{negate} {
}

expression denegator::operator()(caf::none_t) const {
  return caf::none;
}

expression denegator::operator()(const conjunction& c) const {
  auto add = [&](auto x) -> expression {
    for (auto& op : c)
      x.push_back(caf::visit(*this, op));
    return x;
  };
  return negate_ ? add(disjunction{}) : add(conjunction{});
}

expression denegator::operator()(const disjunction& d) const {
  auto add = [&](auto x) -> expression {
    for (auto& op : d)
      x.push_back(caf::visit(*this, op));
    return x;
  };
  return negate_ ? add(conjunction{}) : add(disjunction{});
}

expression denegator::operator()(const negation& n) const {
  // Step through double negations.
  if (auto inner = caf::get_if<negation>(&n.expr()))
    return caf::visit(*this, inner->expr());
  // Apply De Morgan from here downward.
  return caf::visit(denegator{!negate_}, n.expr());
}

expression denegator::operator()(const predicate& p) const {
  return predicate{p.lhs, negate_ ? negate(p.op) : p.op, p.rhs};
}

expression deduplicator::operator()(caf::none_t) const {
  return caf::none;
}

expression deduplicator::operator()(const conjunction& c) const {
  conjunction result;
  for (auto& op : c) {
    auto p = caf::visit(*this, op);
    if (std::count(result.begin(), result.end(), p) == 0)
      result.push_back(p);
  }
  return result;
}

expression deduplicator::operator()(const disjunction& d) const {
  disjunction result;
  for (auto& op : d) {
    auto p = caf::visit(*this, op);
    if (std::count(result.begin(), result.end(), p) == 0)
      result.push_back(p);
  }
  return result;
}

expression deduplicator::operator()(const negation& n) const {
  return caf::visit(*this, n.expr());
}

expression deduplicator::operator()(const predicate& p) const {
  return p;
}

namespace {

template <class Ts, class Us>
auto inplace_union(Ts& xs, const Us& ys) {
  auto mid = xs.size();
  std::copy(ys.begin(), ys.end(), std::back_inserter(xs));
  std::inplace_merge(xs.begin(), xs.begin() + mid, xs.end());
  xs.erase(std::unique(xs.begin(), xs.end()), xs.end());
}

} // namespace

std::vector<predicate> predicatizer::operator()(caf::none_t) const {
  return {};
}

std::vector<predicate> predicatizer::operator()(const conjunction& con) const {
  std::vector<predicate> result;
  for (auto& op : con) {
    auto ps = caf::visit(*this, op);
    inplace_union(result, ps);
  }
  return result;
}

std::vector<predicate> predicatizer::operator()(const disjunction& dis) const {
  std::vector<predicate> result;
  for (auto& op : dis) {
    auto ps = caf::visit(*this, op);
    inplace_union(result, ps);
  }
  return result;
}

std::vector<predicate> predicatizer::operator()(const negation& n) const {
  return caf::visit(*this, n.expr());
}

std::vector<predicate> predicatizer::operator()(const predicate& pred) const {
  return {pred};
}

caf::expected<void> validator::operator()(caf::none_t) {
  return caf::make_error(ec::syntax_error, "nil expression is invalid");
}

caf::expected<void> validator::operator()(const conjunction& c) {
  for (auto& op : c) {
    auto m = caf::visit(*this, op);
    if (!m)
      return m;
  }
  return caf::no_error;
}

caf::expected<void> validator::operator()(const disjunction& d) {
  for (auto& op : d) {
    auto m = caf::visit(*this, op);
    if (!m)
      return m;
  }
  return caf::no_error;
}

caf::expected<void> validator::operator()(const negation& n) {
  return caf::visit(*this, n.expr());
}

caf::expected<void> validator::operator()(const predicate& p) {
  op_ = p.op;
  // If rhs is a pattern, validate early that it is a valid regular expression.
  if (auto dat = caf::get_if<data>(&p.rhs))
    if (auto pat = caf::get_if<pattern>(dat))
      try {
        [[maybe_unused]] auto r = std::regex{pat->string()};
      } catch (const std::regex_error& err) {
        return caf::make_error(
          ec::syntax_error, "failed to create regular expression from pattern",
          pat->string(), err.what());
      }
  return caf::visit(*this, p.lhs, p.rhs);
}

caf::expected<void>
validator::operator()(const meta_extractor& ex, const data& d) {
  if (ex.kind == meta_extractor::type
      && !(caf::holds_alternative<std::string>(d)
           || caf::holds_alternative<pattern>(d)))
    return caf::make_error(ec::syntax_error,
                           "type meta extractor requires string or pattern "
                           "operand",
                           "#type", op_, d);
  if (ex.kind == meta_extractor::field
      && !(caf::holds_alternative<std::string>(d)
           || caf::holds_alternative<pattern>(d)))
    return caf::make_error(ec::syntax_error,
                           "field attribute extractor requires string or "
                           "pattern operand",
                           "#field", op_, d);
  return caf::no_error;
}

caf::expected<void>
validator::operator()(const type_extractor& ex, const data& d) {
  // References to aliases can't be checked here because the expression parser
  // can't possible know about them. We defer the check to the type resolver.
  if (caf::holds_alternative<none_type>(ex.type))
    return caf::no_error;
  if (!compatible(ex.type, op_, d))
    return caf::make_error(
      ec::syntax_error, "type extractor type check failure", ex.type, op_, d);
  return caf::no_error;
}

caf::expected<void> validator::operator()(const field_extractor&, const data&) {
  // Validity of a field extractor requires a specific schema, which we don't
  // have in this context.
  return caf::no_error;
}

type_resolver::type_resolver(const type& t) : type_{t} {
}

caf::expected<expression> type_resolver::operator()(caf::none_t) {
  return expression{};
}

caf::expected<expression> type_resolver::operator()(const conjunction& c) {
  conjunction result;
  for (auto& op : c) {
    auto r = caf::visit(*this, op);
    if (!r)
      return r;
    else if (caf::holds_alternative<caf::none_t>(*r))
      return expression{};
    else
      result.push_back(std::move(*r));
  }
  if (result.empty())
    return expression{};
  if (result.size() == 1)
    return {std::move(result[0])};
  else
    return {std::move(result)};
}

caf::expected<expression> type_resolver::operator()(const disjunction& d) {
  disjunction result;
  for (auto& op : d) {
    auto r = caf::visit(*this, op);
    if (!r)
      return r;
    else if (!caf::holds_alternative<caf::none_t>(*r))
      result.push_back(std::move(*r));
  }
  if (result.empty())
    return expression{};
  if (result.size() == 1)
    return {std::move(result[0])};
  else
    return {std::move(result)};
}

caf::expected<expression> type_resolver::operator()(const negation& n) {
  auto r = caf::visit(*this, n.expr());
  if (!r)
    return r;
  else if (!caf::holds_alternative<caf::none_t>(*r))
    return {negation{std::move(*r)}};
  else
    return expression{};
}

caf::expected<expression> type_resolver::operator()(const predicate& p) {
  op_ = p.op;
  return caf::visit(*this, p.lhs, p.rhs);
}

caf::expected<expression>
type_resolver::operator()(const meta_extractor& ex, const data& d) {
  // We're leaving all attributes alone, because both #type and #field operate
  // at a different granularity.
  return predicate{ex, op_, d};
}

caf::expected<expression>
type_resolver::operator()(const type_extractor& ex, const data& d) {
  if (caf::holds_alternative<none_type>(ex.type)) {
    auto matches = [&](const type& t) {
      const auto* p = &t;
      while (const auto* a = caf::get_if<alias_type>(p)) {
        if (a->name() == ex.type.name())
          return compatible(*a, op_, d);
        p = &a->value_type;
      }
      if (p->name() == ex.type.name())
        return compatible(*p, op_, d);
      return false;
    };
    // Preserve compatibility with databases that were created beore
    // the #timestamp attribute was removed.
    if (ex.type.name() == "timestamp") {
      if (!caf::holds_alternative<time>(d))
        return caf::make_error(ec::type_clash, ":timestamp", op_, d);
      auto has_timestamp_attribute
        = [&](const type& t) { return has_attribute(t, "timestamp"); };
      return disjunction{resolve_extractor(matches, d),
                         resolve_extractor(has_timestamp_attribute, d)};
    }
    return resolve_extractor(matches, d);
  }
  auto is_congruent = [&](const type& t) { return congruent(t, ex.type); };
  return resolve_extractor(is_congruent, d);
}

caf::expected<expression>
type_resolver::operator()(const data& d, const type_extractor& ex) {
  return (*this)(ex, d);
}

caf::expected<expression>
type_resolver::operator()(const field_extractor& ex, const data& d) {
  std::vector<expression> connective;
  // First, interpret the field as a suffix of a record field name.
  if (auto r = caf::get_if<record_type>(&type_)) {
    auto suffixes = r->find_suffix(ex.field);
    for (auto& offset : suffixes) {
      auto f = r->at(offset);
      if (!compatible(f->type, op_, d))
        continue;
      auto x = data_extractor{f->type, std::move(offset)};
      connective.emplace_back(predicate{std::move(x), op_, d});
    }
    // Second, try to interpret the field as the name of a single type.
  } else if (ex.field == type_.name()) {
    if (!compatible(type_, op_, d))
      return caf::make_error(ec::type_clash, type_, op_, d);
    auto x = data_extractor{type_, {}};
    connective.emplace_back(predicate{std::move(x), op_, d});
  }
  if (connective.empty())
    return expression{}; // did not resolve
  if (connective.size() == 1)
    return {std::move(connective[0])};
  if (op_ == relational_operator::not_equal
      || op_ == relational_operator::not_match
      || op_ == relational_operator::not_in
      || op_ == relational_operator::not_ni)
    return {conjunction{std::move(connective)}};
  else
    return {disjunction{std::move(connective)}};
}

caf::expected<expression>
type_resolver::operator()(const data& d, const field_extractor& ex) {
  return (*this)(ex, d);
}

matcher::matcher(const type& t) : type_{t} {
  // nop
}

bool matcher::operator()(caf::none_t) {
  return false;
}

bool matcher::operator()(const conjunction& c) {
  for (auto& op : c)
    if (!caf::visit(*this, op))
      return false;
  return true;
}

bool matcher::operator()(const disjunction& d) {
  for (auto& op : d)
    if (caf::visit(*this, op))
      return true;
  return false;
}

bool matcher::operator()(const negation& n) {
  return caf::visit(*this, n.expr());
}

bool matcher::operator()(const predicate& p) {
  op_ = p.op;
  return caf::visit(*this, p.lhs, p.rhs);
}

bool matcher::operator()(const meta_extractor& e, const data& d) {
  if (e.kind == meta_extractor::type) {
    VAST_ASSERT(caf::holds_alternative<std::string>(d));
    return evaluate(d, op_, type_.name());
  }
  return false;
}

bool matcher::operator()(const data_extractor&, const data&) {
  // If we encounter a data_extractor, it must have been created through a
  // previous invocation of a type_resolver visitation. The presence of
  // data_extractor indicates that the expression matches.
  return true;
}

} // namespace vast
