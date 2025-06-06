//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/expression.hpp"

#include "tenzir/concept/printable/tenzir/expression.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/expression_visitors.hpp"
#include "tenzir/logger.hpp"

namespace tenzir {

// -- meta_extractor -----------------------------------------------------------

bool operator==(const meta_extractor& x, const meta_extractor& y) {
  return x.kind == y.kind;
}

bool operator<(const meta_extractor& x, const meta_extractor& y) {
  return x.kind < y.kind;
}

// -- field_extractor ----------------------------------------------------------

field_extractor::field_extractor(std::string f) : field{std::move(f)} {
}

bool operator==(const field_extractor& x, const field_extractor& y) {
  return x.field == y.field;
}

bool operator<(const field_extractor& x, const field_extractor& y) {
  return x.field < y.field;
}

// -- type_extractor -----------------------------------------------------------

type_extractor::type_extractor(tenzir::type t) : type{std::move(t)} {
}

bool operator==(const type_extractor& x, const type_extractor& y) {
  return x.type == y.type;
}

bool operator<(const type_extractor& x, const type_extractor& y) {
  return x.type < y.type;
}

// -- data_extractor -----------------------------------------------------------

data_extractor::data_extractor(class type t, size_t column)
  : type{std::move(t)}, column{column} {
}

data_extractor::data_extractor(const record_type& rt, const offset& o)
  : type{rt.field(o).type}, column{rt.flat_index(o)} {
}

bool operator==(const data_extractor& x, const data_extractor& y) {
  return x.type == y.type && x.column == y.column;
}

bool operator<(const data_extractor& x, const data_extractor& y) {
  return std::tie(x.type, x.column) < std::tie(y.type, y.column);
}

// -- predicate ----------------------------------------------------------------

predicate::predicate(operand l, relational_operator o, operand r)
  : lhs{std::move(l)}, op{o}, rhs{std::move(r)} {
}

bool operator==(const predicate& x, const predicate& y) {
  return x.lhs == y.lhs && x.op == y.op && x.rhs == y.rhs;
}

bool operator<(const predicate& x, const predicate& y) {
  return std::tie(x.lhs, x.op, x.rhs) < std::tie(y.lhs, y.op, y.rhs);
}

// -- curried_predicate --------------------------------------------------------

curried_predicate curried(const predicate& pred) {
  TENZIR_ASSERT(is<data>(pred.rhs));
  return {pred.op, as<data>(pred.rhs)};
}

// -- conjunction --------------------------------------------------------------

conjunction::conjunction(const super& other) : super{other} {
  // nop
}

conjunction::conjunction(super&& other) noexcept : super{std::move(other)} {
  // nop
}

// -- disjunction --------------------------------------------------------------

disjunction::disjunction(const super& other) : super{other} {
  // nop
}

disjunction::disjunction(super&& other) noexcept : super{std::move(other)} {
  // nop
}

// -- negation -----------------------------------------------------------------

negation::negation() : expr_{std::make_unique<expression>()} {
}

negation::negation(expression expr)
  : expr_{std::make_unique<expression>(std::move(expr))} {
}

negation::negation(const negation& other)
  : expr_{std::make_unique<expression>(*other.expr_)} {
}

negation::negation(negation&& other) noexcept : expr_{std::move(other.expr_)} {
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

expression hoist(expression expr) {
  return match(std::move(expr), hoister{});
}

expression prune_meta_predicates(expression expr) {
  return match(std::move(expr), meta_pruner{});
}

expression normalize(expression expr) {
  expr = match(std::move(expr), hoister{});
  expr = match(std::move(expr), aligner{});
  expr = match(std::move(expr), denegator{});
  expr = match(std::move(expr), deduplicator{});
  expr = match(std::move(expr), hoister{});
  return expr;
}

caf::expected<expression> normalize_and_validate(expression expr) {
  expr = normalize(std::move(expr));
  if (auto result = match(std::move(expr), validator{}); !result)
    return result.error();
  return expr;
}

caf::expected<expression> tailor(expression expr, const type& schema) {
  TENZIR_ASSERT(is<record_type>(schema));
  if (is<caf::none_t>(expr)) {
    return caf::make_error(ec::unspecified, fmt::format("unable to tailor "
                                                        "empty expression"));
  }
  auto result = match(std::move(expr), type_resolver{schema});
  if (!result)
    return result;
  if (is<caf::none_t>(*result)) {
    return caf::make_error(ec::unspecified, fmt::format("failed to tailor "
                                                        "expression {} for "
                                                        "schema {}",
                                                        expr, schema));
  }
  return result;
}

namespace {

// Helper function to lookup an expression at a particular offset
const expression* at(const expression* expr, offset::value_type i) {
  TENZIR_ASSERT(expr != nullptr);
  auto f = detail::overload{
    [&](const conjunction& xs) -> const expression* {
      return i < xs.size() ? &xs[i] : nullptr;
    },
    [&](const disjunction& xs) -> const expression* {
      return i < xs.size() ? &xs[i] : nullptr;
    },
    [&](const negation& x) -> const expression* {
      return i == 0 ? &x.expr() : nullptr;
    },
    [&](const auto&) -> const expression* {
      return nullptr;
    },
  };
  return match(*expr, f);
}

} // namespace

const expression* at(const expression& expr, const offset& o) {
  if (o.empty())
    return nullptr; // empty offsets are invalid
  if (o.size() == 1)
    return o[0] == 0 ? &expr : nullptr; // the root has always offset [0]
  auto ptr = &expr;
  for (size_t i = 1; i < o.size(); ++i) {
    ptr = at(ptr, o[i]);
    if (ptr == nullptr)
      break;
  }
  return ptr;
}

namespace {

bool resolve_impl(std::vector<std::pair<offset, predicate>>& result,
                  const expression& expr, const type& t, offset& o) {
  auto v = detail::overload{
    [&](const auto& xs) { // conjunction or disjunction
      o.emplace_back(0);
      if (!xs.empty()) {
        if (!resolve_impl(result, xs[0], t, o))
          return false;
        for (size_t i = 1; i < xs.size(); ++i) {
          o.back() += 1;
          if (!resolve_impl(result, xs[i], t, o))
            return false;
        }
      }
      o.pop_back();
      return true;
    },
    [&](const negation& x) {
      o.emplace_back(0);
      if (!resolve_impl(result, x.expr(), t, o))
        return false;
      o.pop_back();
      return true;
    },
    [&](const predicate& x) {
      auto resolved = type_resolver{t}(x);
      // Abort on first type error and return a
      // default-constructed vector.
      if (!resolved)
        return false;
      for (auto& pred : match(*resolved, predicatizer{}))
        result.emplace_back(o, std::move(pred));
      return true;
    },
    [&](caf::none_t) {
      return false;
    },
  };
  return match(expr, v);
}

} // namespace

std::vector<std::pair<offset, predicate>>
resolve(const expression& expr, const type& t) {
  std::vector<std::pair<offset, predicate>> result;
  offset o{0};
  if (resolve_impl(result, expr, t, o))
    return result;
  return {};
}

} // namespace tenzir
