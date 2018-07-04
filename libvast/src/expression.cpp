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

// -- attribute_extractor ------------------------------------------------------

attribute_extractor::attribute_extractor(std::string str)
  : attr{std::move(str)} {
}

bool operator==(const attribute_extractor& x, const attribute_extractor& y) {
  return x.attr == y.attr;
}

bool operator<(const attribute_extractor& x, const attribute_extractor& y) {
  return x.attr < y.attr;
}

// -- key_extractor ------------------------------------------------------------

key_extractor::key_extractor(vast::key k) : key{std::move(k)} {
}

bool operator==(const key_extractor& x, const key_extractor& y) {
  return x.key == y.key;
}

bool operator<(const key_extractor& x, const key_extractor& y) {
  return x.key < y.key;
}

// -- type_extractor -----------------------------------------------------------

type_extractor::type_extractor(vast::type t) : type{std::move(t)} {
}

bool operator==(const type_extractor& x, const type_extractor& y) {
  return x.type == y.type;
}

bool operator<(const type_extractor& x, const type_extractor& y) {
  return x.type < y.type;
}

// -- data_extractor -----------------------------------------------------------

data_extractor::data_extractor(vast::type t, vast::offset o)
  : type{std::move(t)}, offset{std::move(o)} {
}

bool operator==(const data_extractor& x, const data_extractor& y) {
  return x.type == y.type && x.offset == y.offset;
}

bool operator<(const data_extractor& x, const data_extractor& y) {
  return std::tie(x.type, x.offset) < std::tie(y.type, y.offset);
}

// -- predicate ----------------------------------------------------------------

predicate::predicate(operand l, relational_operator o, operand r)
  : lhs{std::move(l)}, op{o}, rhs{std::move(r)} {
}

bool operator==(const predicate& x, const predicate& y) {
  return x.lhs == y.lhs && x.op == y.op && x.rhs == y.rhs;
}

bool operator<(const predicate& x, const predicate& y) {
  return std::tie(x.lhs, x.op, x.rhs)
         < std::tie(y.lhs, y.op, y.rhs);
}

// -- negation -----------------------------------------------------------------

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

bool operator==(const negation& x, const negation& y) {
  return x.expr() == y.expr();
}

bool operator<(const negation& x, const negation& y) {
  return x.expr() < y.expr();
}

// -- expression ---------------------------------------------------------------

const expression::node& expression::get_data() const {
  return node_;
}

expression::node& expression::get_data() {
  return node_;
}

bool operator==(const expression& x, const expression& y) {
  return x.get_data() == y.get_data();
}

bool operator<(const expression& x, const expression& y) {
  return x.get_data() < y.get_data();
}

// -- free functions -----------------------------------------------------------

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
  if (caf::holds_alternative<caf::none_t>(expr))
    return make_error(ec::unspecified, "invalid expression");
  auto x = caf::visit(type_resolver{t}, expr);
  if (!x)
    return x.error();
  *x = caf::visit(type_pruner{t}, *x);
  VAST_ASSERT(!caf::holds_alternative<caf::none_t>(*x));
  return std::move(*x);
}
} // namespace vast
