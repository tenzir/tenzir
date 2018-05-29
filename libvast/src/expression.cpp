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

#include "vast/expression.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/detail/assert.hpp"

namespace vast {

attribute_extractor::attribute_extractor(std::string str)
  : attr{std::move(str)} {
}

bool operator==(const attribute_extractor& x, const attribute_extractor& y) {
  return x.attr == y.attr;
}

bool operator<(const attribute_extractor& x, const attribute_extractor& y) {
  return x.attr < y.attr;
}

key_extractor::key_extractor(vast::key k) : key{std::move(k)} {
}

bool operator==(const key_extractor& lhs, const key_extractor& rhs) {
  return lhs.key == rhs.key;
}

bool operator<(const key_extractor& lhs, const key_extractor& rhs) {
  return lhs.key < rhs.key;
}

type_extractor::type_extractor(vast::type t) : type{std::move(t)} {
}

bool operator==(const type_extractor& lhs, const type_extractor& rhs) {
  return lhs.type == rhs.type;
}

bool operator<(const type_extractor& lhs, const type_extractor& rhs) {
  return lhs.type < rhs.type;
}

data_extractor::data_extractor(vast::type t, vast::offset o)
  : type{std::move(t)}, offset{std::move(o)} {
}

bool operator==(const data_extractor& lhs, const data_extractor& rhs) {
  return lhs.type == rhs.type && lhs.offset == rhs.offset;
}

bool operator<(const data_extractor& lhs, const data_extractor& rhs) {
  return std::tie(lhs.type, lhs.offset) < std::tie(rhs.type, rhs.offset);
}

predicate::predicate(operand l, relational_operator o, operand r)
  : lhs{std::move(l)}, op{o}, rhs{std::move(r)} {
}

bool operator==(const predicate& lhs, const predicate& rhs) {
  return lhs.lhs == rhs.lhs && lhs.op == rhs.op && lhs.rhs == rhs.rhs;
}

bool operator<(const predicate& lhs, const predicate& rhs) {
  return std::tie(lhs.lhs, lhs.op, lhs.rhs)
         < std::tie(rhs.lhs, rhs.op, rhs.rhs);
}

negation::negation()
  : expr_{std::make_unique<expression>()} {
}

negation::negation(expression expr)
  : expr_{std::make_unique<expression>(std::move(expr))} {
}

negation::negation(const negation& other)
  : expr_{std::make_unique<expression>(*other.expr_)} {
}

negation::negation(negation&& other) noexcept
  : expr_{std::move(other.expr_)} {
}

negation& negation::operator=(const negation& other) {
  *expr_ = *other.expr_;
  return *this;
}

negation& negation::operator=(negation&& other) noexcept {
  expr_ = std::move(other.expr_);
  return *this;
}

const expression& negation::expr() const {
  return *expr_;
}

expression& negation::expr() {
  return *expr_;
}

expression normalize(const expression& expr) {
  expression r;
  r = caf::visit(hoister{}, expr);
  r = caf::visit(aligner{}, r);
  r = caf::visit(denegator{}, r);
  r = caf::visit(deduplicator{}, r);
  r = caf::visit(hoister{}, r);
  return r;
}

expected<expression> normalize_and_validate(const expression& expr) {
  auto normalized = normalize(expr);
  auto result = caf::visit(validator{}, normalized);
  if (!result)
    return result.error();
  return normalized;
}

expected<expression> tailor(const expression& expr, const type& t) {
  if (caf::holds_alternative<none>(expr))
    return make_error(ec::unspecified, "invalid expression");
  auto x = caf::visit(type_resolver{t}, expr);
  if (!x)
    return x.error();
  *x = caf::visit(type_pruner{t}, *x);
  VAST_ASSERT(!caf::holds_alternative<none>(*x));
  return std::move(*x);
}

const expression::node& expression::get_data() const {
  return node_;
}

expression::node& expression::get_data() {
  return node_;
}

bool operator==(const negation& lhs, const negation& rhs) {
  return *lhs.expr_ == *rhs.expr_;
}

bool operator<(const negation& lhs, const negation& rhs) {
  return *lhs.expr_ < *rhs.expr_;
}

bool operator==(const expression& lhs, const expression& rhs) {
  return lhs.node_ == rhs.node_;
}

bool operator<(const expression& lhs, const expression& rhs) {
  return lhs.node_ < rhs.node_;
}
} // namespace vast
