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

bool operator==(attribute_extractor const& x, attribute_extractor const& y) {
  return x.attr == y.attr;
}

bool operator<(attribute_extractor const& x, attribute_extractor const& y) {
  return x.attr < y.attr;
}

key_extractor::key_extractor(vast::key k) : key{std::move(k)} {
}

bool operator==(key_extractor const& lhs, key_extractor const& rhs) {
  return lhs.key == rhs.key;
}

bool operator<(key_extractor const& lhs, key_extractor const& rhs) {
  return lhs.key < rhs.key;
}

type_extractor::type_extractor(vast::type t) : type{std::move(t)} {
}

bool operator==(type_extractor const& lhs, type_extractor const& rhs) {
  return lhs.type == rhs.type;
}

bool operator<(type_extractor const& lhs, type_extractor const& rhs) {
  return lhs.type < rhs.type;
}

data_extractor::data_extractor(vast::type t, vast::offset o)
  : type{std::move(t)}, offset{std::move(o)} {
}

bool operator==(data_extractor const& lhs, data_extractor const& rhs) {
  return lhs.type == rhs.type && lhs.offset == rhs.offset;
}

bool operator<(data_extractor const& lhs, data_extractor const& rhs) {
  return std::tie(lhs.type, lhs.offset) < std::tie(rhs.type, rhs.offset);
}

predicate::predicate(operand l, relational_operator o, operand r)
  : lhs{std::move(l)}, op{o}, rhs{std::move(r)} {
}

bool operator==(predicate const& lhs, predicate const& rhs) {
  return lhs.lhs == rhs.lhs && lhs.op == rhs.op && lhs.rhs == rhs.rhs;
}

bool operator<(predicate const& lhs, predicate const& rhs) {
  return std::tie(lhs.lhs, lhs.op, lhs.rhs)
         < std::tie(rhs.lhs, rhs.op, rhs.rhs);
}

negation::negation()
  : expr_{std::make_unique<expression>()} {
}

negation::negation(expression expr)
  : expr_{std::make_unique<expression>(std::move(expr))} {
}

negation::negation(negation const& other)
  : expr_{std::make_unique<expression>(*other.expr_)} {
}

negation::negation(negation&& other) noexcept
  : expr_{std::move(other.expr_)} {
}

negation& negation::operator=(negation const& other) {
  *expr_ = *other.expr_;
  return *this;
}

negation& negation::operator=(negation&& other) noexcept {
  expr_ = std::move(other.expr_);
  return *this;
}

expression const& negation::expr() const {
  return *expr_;
}

expression& negation::expr() {
  return *expr_;
}


bool operator==(negation const& lhs, negation const& rhs) {
  return *lhs.expr_ == *rhs.expr_;
}

bool operator<(negation const& lhs, negation const& rhs) {
  return *lhs.expr_ < *rhs.expr_;
}

bool operator==(expression const& lhs, expression const& rhs) {
  return lhs.node_ == rhs.node_;
}

bool operator<(expression const& lhs, expression const& rhs) {
  return lhs.node_ < rhs.node_;
}

expression::node& expose(expression& e) {
  return e.node_;
}

expression normalize(expression const& expr) {
  expression r;
  r = visit(hoister{}, expr);
  r = visit(aligner{}, r);
  r = visit(denegator{}, r);
  r = visit(deduplicator{}, r);
  r = visit(hoister{}, r);
  return r;
}

expected<expression> normalize_and_validate(const expression& expr) {
  auto normalized = normalize(expr);
  auto result = visit(validator{}, normalized);
  if (!result)
    return result.error();
  return normalized;
}

expected<expression> tailor(const expression& expr, const type& t) {
  if (is<none>(expr))
    return make_error(ec::unspecified, "invalid expression");
  auto x = visit(type_resolver{t}, expr);
  if (!x)
    return x.error();
  *x = visit(type_pruner{t}, *x);
  VAST_ASSERT(!is<none>(*x));
  return std::move(*x);
}

} // namespace vast
